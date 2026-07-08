"""Live smoke test: exercise WatchResource against a real Kubernetes cluster.

Unlike the rest of the suite (fully offline, respx-mocked), this test proves the
extracted module still works end-to-end against an actual API server. It runs as
part of the default `pytest` invocation, but automatically skips itself if no
cluster is reachable at `KUBECONFIG` (e.g. `integration-test-cluster/start_cluster.sh`
hasn't been run).
"""

import asyncio
import subprocess
import uuid
from pathlib import Path

import pytest

import k8s_watch

KUBECONFIG = Path("/tmp/kubeconfig-kind.yaml")


def _kubectl(*args: str) -> None:
    subprocess.run(
        ["kubectl", f"--kubeconfig={KUBECONFIG}", *args],
        check=True,
        capture_output=True,
        text=True,
    )


def _cluster_available() -> bool:
    """Best-effort check so the suite can run without a live cluster."""
    try:
        _kubectl("cluster-info", "--request-timeout=3s")
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False
    return True


pytestmark = pytest.mark.skipif(
    not _cluster_available(), reason="no reachable Kubernetes cluster"
)


async def test_watch_namespace_lifecycle():
    """Watching /api/v1/namespaces must observe a real create + delete."""
    ns_name = f"k8s-watch-live-{uuid.uuid4().hex[:8]}"

    k8scfg, err = k8s_watch.create_cluster_config(KUBECONFIG, context="kind-kind")
    assert not err

    async with (
        k8scfg.client,
        k8s_watch.WatchResource(k8scfg, "/api/v1/namespaces") as watch,
    ):
        # Create/remove a namespace.
        _kubectl("create", "namespace", ns_name)
        _kubectl("delete", "namespace", ns_name, "--wait=false")

        async def collect():
            async for data in watch:
                evt, manifest = data.type, data.object
                if manifest["metadata"]["name"] == ns_name and evt == "ADDED":
                    return

        try:
            await asyncio.wait_for(collect(), timeout=5)
        except asyncio.TimeoutError:
            assert False, f"never observed ADDED for namespace {ns_name}"
