import os
import pathlib

import pytest

from app.config import get_config
from tests.conftest import file_path

pytestmark = pytest.mark.vertica

base_url = "/v3/connector/vertica"

function_list_path = file_path("../resources/function_list")


def pytest_collection_modifyitems(items):
    current_file_dir = pathlib.Path(__file__).resolve().parent
    for item in items:
        if pathlib.Path(item.fspath).is_relative_to(current_file_dir):
            item.add_marker(pytestmark)


@pytest.fixture(scope="session")
def connection_info():
    return {
        "host": os.getenv("TEST_VERTICA_HOST", "localhost"),
        "port": os.getenv("TEST_VERTICA_PORT", "5433"),
        "database": os.getenv("TEST_VERTICA_DATABASE", "mydb"),
        "user": os.getenv("TEST_VERTICA_USER", "dbadmin"),
        "password": os.getenv("TEST_VERTICA_PASSWORD", ""),
    }


@pytest.fixture(autouse=True)
def set_remote_function_list_path():
    config = get_config()
    config.set_remote_function_list_path(function_list_path)
    yield
    config.set_remote_function_list_path(None)
