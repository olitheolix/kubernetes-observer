[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)
![Python 3.14, 3.13](https://img.shields.io/badge/python-3.14,3.13-blue.svg)
[![CI](https://github.com/olitheolix/kubernetes-observer/actions/workflows/ci.yml/badge.svg)](https://github.com/olitheolix/kubernetes-observer/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/olitheolix/kubernetes-observer.svg?style=flat)](https://codecov.io/gh/olitheolix/kubernetes-observer)


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

# `create_cluster_config` attaches an HTTPX client to `k8scfg.client`; close it
# on exit (alongside the watcher) so its connection pool does not leak.
async with (
    k8scfg.client,
    k8s_watch.WatchResource(k8scfg, "/api/v1/namespaces") as watch,
):
    async for event in watch:
        manifest = event.object
        print(event.type, manifest["metadata"]["name"])
```

## Development

```bash
# Spin up a local KinD cluster.
integration-test-cluster/start_cluster.sh

# Run Test Suite
uv sync
uv run pytest

# Linting
uvx ruff check && uvx ruff format && uvx ty check
```

### Demo

With the KinD cluster running, watch namespace events live:

```console
$ uv run examples/watch_namespaces.py
ADDED kube-system
ADDED default
ADDED kube-public
ADDED foo
MODIFIED foo
MODIFIED foo
DELETED foo
```

It prints every event on `/api/v1/namespaces` until you press Ctrl-C. To see
events flow, create and delete a namespace from another terminal:

```bash
kubectl --kubeconfig=/tmp/kubeconfig-kind.yaml create namespace demo
kubectl --kubeconfig=/tmp/kubeconfig-kind.yaml delete namespace demo
```

## Why
I have used and tweaked this code over the years in various projects with minor
modifications in each. Now I finally decided to create one canonical version.
This is mostly useful to me, but maybe others find it useful too.
