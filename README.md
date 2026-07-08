# kubernetes-observer

Async Kubernetes resource watcher (`k8s_watch` module).

`k8s_watch.WatchResource` watches a single K8s resource collection (e.g.
`/api/v1/namespaces`) and yields `ADDED` / `MODIFIED` / `DELETED` events as an
async iterator, automatically reconnecting on drops or `410 Gone` responses.

## Usage

```python
import logging
from pathlib import Path
import k8s_watch

# Show the watcher's INFO logs (reconnects, closed connections, etc.).
logging.basicConfig(level=logging.INFO)

k8scfg, err = k8s_watch.create_cluster_config(Path("/path/to/kubeconfig.yaml"), "my-context")
assert not err

async with k8s_watch.WatchResource(k8scfg, "/api/v1/namespaces") as watch:
    async for event in watch:
        manifest = event.object
        print(event.type, manifest["metadata"]["name"])
```

## Development

```bash
uv sync
integration-test-cluster/start_cluster.sh   # spin up a local KinD cluster
uv run pytest                               # runs the full suite, including the live test
```

`start_cluster.sh` creates a KinD cluster and writes its kubeconfig to
`/tmp/kubeconfig-kind.yaml` (context `kind-kind`).
