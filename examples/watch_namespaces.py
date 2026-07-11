"""Demo: watch namespace events against the local KinD cluster.

Start the cluster first::

    integration-test-cluster/start_cluster.sh

Then run this demo::

    uv run python examples/watch_namespaces.py

It watches ``/api/v1/namespaces`` and prints every event until you press Ctrl-C.
To see events flow, create and delete a namespace from another terminal::

    kubectl --kubeconfig=/tmp/kubeconfig-kind.yaml create namespace demo
    kubectl --kubeconfig=/tmp/kubeconfig-kind.yaml delete namespace demo
"""

import asyncio
import logging
from pathlib import Path

import k8s_watch

KUBECONFIG = Path("/tmp/kubeconfig-kind.yaml")
CONTEXT = "kind-kind"


async def main() -> None:
    # Show the watcher's INFO logs (reconnects, closed connections, etc.).
    logging.basicConfig(level=logging.INFO)

    k8scfg, err = k8s_watch.create_cluster_config(KUBECONFIG, CONTEXT)
    assert not err

    print("Watching /api/v1/namespaces (Ctrl-C to stop) ...")
    async with (
        k8scfg.client,
        k8s_watch.WatchResource(k8scfg, "/api/v1/namespaces") as watch,
    ):
        async for event in watch:
            manifest = event.object
            print(event.type, manifest["metadata"]["name"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
