import os

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from app.main import app


def file_path(path: str) -> str:
    return os.path.join(os.path.dirname(__file__), path)


DATAFUSION_FUNCTION_COUNT = 285


@pytest_asyncio.fixture(scope="session")
async def client() -> AsyncClient:
    async with LifespanManager(app, startup_timeout=30) as manager:
        async with AsyncClient(
            transport=ASGITransport(manager.app), base_url="http://test"
        ) as client:
            yield client
