# busflow — 本地公交刷卡事件处理工具

读取 JSONL 刷卡事件文件（兼容两个输入版本），归一化为统一格式，按日期分区写入
Parquet，并用 DuckDB 生成线路 × 小时级统计（刷卡次数、去重乘客数、异常记录数）。

## 安装

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt   # duckdb + pytest
```

## 输入格式

**v1（扁平）**

```json
{"event_id": "E1", "line": "42", "station": "Central",
 "board_time": "2026-11-01 08:15:00", "receive_time": "2026-11-01 08:16:10",
 "card_id": "C100"}
```

**v2（嵌套，ISO-8601）**

```json
{"schema_version": 2, "id": "E2",
 "route": {"line": "42", "station": "Central"},
 "timestamps": {"boarded_at": "2026-11-01T08:15:00Z",
                "received_at": "2026-11-01T08:16:10Z"},
 "card": "C100"}
```

两个版本逐条自动识别、归一化为统一事件：`event_id, line, station, board_time,
receive_time, card_id`。带时区的时间戳转换为 naive UTC。必要字段为
`event_id / line / station / board_time / receive_time`；`card_id` 可选，
缺失时去重乘客数回退为按 `event_id` 去重。

## 命令

```bash
python -m busflow --data-dir DATA ingest FILE...          # 幂等导入
python -m busflow --data-dir DATA status                  # 查看处理状态
python -m busflow --data-dir DATA stats [--date DATE]     # 查看统计
python -m busflow --data-dir DATA recompute --all         # 全部重算
python -m busflow --data-dir DATA recompute --date 2026-11-01
python -m busflow --data-dir DATA reprocess-quarantine \
    [--strategy keep-existing|use-quarantined|drop]       # 重新处理隔离记录
```

试用样例：`samples/` 下有 v1、v2、迟到事件和坏数据四个样例文件。

## 数据目录布局

```
DATA/
  ledger.db                            SQLite 账本（事件真源、隔离区、导入日志）
  events/date=YYYY-MM-DD/data.parquet  按上车日期分区
  stats/line_hourly.parquet            线路×小时统计
```

统计表结构：`event_date, line, hour, swipes, unique_passengers, anomalies`。
无法归属到具体日期/线路/小时的异常记录归入对应键为 NULL 的行，不会静默丢失。

## 一致性保证

- **重复导入不计数**：文件内容哈希已提交的导入直接跳过；不同文件中的相同
  事件按 `event_id` + 内容哈希去重。
- **冲突隔离**：同一 `event_id` 内容不同的记录进入隔离区（保留来源文件、行号、
  原因、原始行），已有事件不受影响；可用 `reprocess-quarantine --strategy
  use-quarantined` 以隔离版本覆盖，或 `--strategy drop` 丢弃。
- **坏数据隔离**：JSON 解析失败、缺必要字段、时间戳非法的记录同样进入隔离区，
  并尽力提取日期/线路/小时用于异常统计归属。
- **迟到事件**：分区与统计全部从账本派生。迟到事件到达后仅重建受影响日期的
  分区并全量刷新统计，结果与一次性导入全部数据完全一致（有测试保证）。
- **崩溃恢复**：所有派生文件先写临时文件再原子改名；导入日志记录
  `started → events_committed → committed` 进度，`stats_dirty` 标记统计滞后。
  每次操作前 `recover()` 清理临时文件、回滚未应用的导入、补建已应用导入的
  分区与统计，不会留下半成品分区或重复累计。

## 测试

```bash
.venv/bin/python -m pytest -q
```

覆盖：版本转换、重复导入、事件冲突、迟到数据、坏数据、崩溃恢复（账本提交后 /
分区写入中 / 应用前崩溃）、隔离区重处理、按日期重算、CLI 端到端。
