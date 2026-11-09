import pytest

from busflow.store import Store


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path / "data"


@pytest.fixture
def store(data_dir):
    s = Store(data_dir)
    yield s
    s.close()
