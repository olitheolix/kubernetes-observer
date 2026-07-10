import pytest
from httpx import AsyncClient
from square.dtypes import K8sConfig


@pytest.fixture
async def k8scfg(respx_mock):
    """Return an async test client."""
    async with AsyncClient(base_url="https:") as client:
        yield K8sConfig(client=client)
