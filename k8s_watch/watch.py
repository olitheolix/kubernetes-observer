"""Watch a K8s resource.

This is a producer-consumer setup. The producer tails K8s and puts the events
into a local queue. The consumer is the iterator itself, ie the `__anext__`
method, which pulls from the queue until it receives a `__CANCELLED__` or
`__EXCEPTION__` sentinel.

The background task keeps itself running indefinitely. The only exception is an
`asyncio.CancelledError`, which forces it to shut down.

The background task first fetches all resources to determine the most recent
resource version. It then opens a long lived connection to the watch endpoint,
starting from that resource version, and forwards the event stream into the
local queue. This continues until the connection dies or K8s responds with 410
(Gone). At that point the task closes the connection, records the most recent
resource version and starts over.

"""

import asyncio
import json
import logging
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypedDict, cast, get_args
import ssl

import httpx
import square.k8s
from square.dtypes import ConnectionParameters, K8sConfig

WEB_EXCEPTIONS = (httpx.RequestError, ssl.SSLError, asyncio.TimeoutError)
EventType = Literal["ADDED", "MODIFIED", "DELETED", "BOOKMARK"]

# The subset of events that `update_state` is allowed to process.
MutationType = Literal["ADDED", "MODIFIED", "DELETED"]


@dataclass(frozen=True, slots=True)
class WatchEvent:
    """A single change event yielded by the iterator.

    `type` is the kind of change (ADDED/MODIFIED/DELETED) and `object` is the
    corresponding K8s manifest.
    """

    type: EventType
    object: dict


class K8sLineJSON(TypedDict):
    """A single JSON encoded line from the K8s watch stream."""

    type: EventType
    object: dict


class WatchResource:
    """Track resource changes over time.

    Usage:

    kubeconfig = Path("/tmp/kind-kubeconf.yaml")
    kubecontext = "kind-kind"
    k8scfg, err = k8s_watch.create_cluster_config(kubeconfig, kubecontext)
    assert not err

    # Use `async with` so the background watch task is always cancelled and
    # awaited on exit; a bare `async for` that breaks early would leak it.
    async with k8s_watch.WatchResource(k8scfg, "/api/v1/namespaces") as watch:
        async for event in watch:
            manifest = event.object
            print(event.type, manifest["metadata"]["name"])

    """

    def __init__(
        self,
        k8scfg: K8sConfig,
        path: str,
        rv: int = -1,
        timeout: int = 300,
        logger: logging.Logger = logging.getLogger("Watch"),
    ):
        self.logit = logger
        self.k8scfg = k8scfg

        self.last_rv: int = rv  # Last seen resource version.
        self.list_path: str = path  # Resource path, eg "/api/crt/v1/namespaces"
        # Server-side watch timeout in seconds. K8s closes the watch cleanly
        # after this long so we reconnect and resume from `last_rv`. Keep it
        # well below the client read timeout (see `create_cluster_config`), else
        # a quiet watch trips the client timeout as an error instead.
        self.timeout = timeout
        self.queue: asyncio.Queue = asyncio.Queue()

        # Track our current knowledge as a `{UID: manifest}` dict.
        self.state: dict[str, dict] = {}

        self.watch_path = f"{path}?watch=true&timeoutSeconds={self.timeout}"

        # Start the background tasks.
        self.tasks = self.start_tasks()

    def start_tasks(self):
        return [asyncio.create_task(self.background_runner())]

    def stop_tasks(self):
        for task in self.tasks:
            task.cancel()

    def __aiter__(self):  # codecov-skip
        return self

    async def __anext__(self):
        while True:
            # Wait for the next event.
            event = await self.queue.get()

            # If the background task was cancelled or raised an unhandled
            # exception we will stop the iterator.
            if event in ("__EXCEPTION__", "__CANCELLED__"):
                self.tasks[0].result()  # NOTE: raises exception if there was one.
                raise StopAsyncIteration
            return event

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        # Request cancellation and then *wait* for the background task(s) to
        # unwind. Without the await, teardown races the caller closing the
        # HTTP client (see the `async with (client, watch)` usage), leaving the
        # task pending mid-stream.
        self.stop_tasks()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def get_logging_metadata(self) -> dict:
        url = self.k8scfg.client.base_url
        host = str(url) if url else ""
        meta_log = {
            "component": "k8s-watch",
            "path": self.list_path,
            "host": host,
        }
        return meta_log

    def construct_watch_path(self, rv: int) -> str:
        return f"{self.watch_path}&resourceVersion={rv}"

    async def list_resource(self) -> tuple[int, bool]:
        """Download the latest manifests of the resource and return the last RV.

        This also syncs the internal state and emits synthetic events to update
        the old state to the new one.

        """
        # Fetch the current set of manifests. `square.k8s.get` logs the failure
        # details on its own "square" logger, but re-log here so the failure
        # also surfaces on the caller-configured watch logger.
        ret, err = await square.k8s.get(self.k8scfg, self.list_path)
        if err:
            meta_log = self.get_logging_metadata()
            self.logit.error("LIST request failed", extra=meta_log)
            return (-1, True)

        # `square.k8s.get` reports success for *any* 200 JSON body, regardless
        # of its shape. Guard against a 200 response that is not a resource
        # collection (eg a single object or an aggregated-API payload) so a
        # malformed body reconnects cleanly instead of killing the watch.
        try:
            items = ret["items"]
            last_resver = int(ret["metadata"]["resourceVersion"])
        except (KeyError, TypeError, ValueError):
            meta_log = self.get_logging_metadata()
            meta_log["k8s_msg"] = ret
            self.logit.error(
                "LIST response is not a valid resource collection", extra=meta_log
            )
            return (-1, True)

        # Sync the current state with the new manifests.
        await self.reset_state(items, self.state)

        return last_resver, False

    async def reset_state(
        self, manifests: list[dict], old_state: dict[str, dict]
    ) -> None:
        """Reset the state to the new `manifests`.

        This emits the corresponding ADDED, MODIFIED and DELETED events to
        transition `old_state` into one that represents the current set of
        `manifests`.

        Only `list_resource` should call this method, typically to sync the
        internal state after a reconnect to K8s.

        Note that `old_state` represents the current state and is independent of
        the iterator that yields the change events. The internal state of this
        class may therefore run ahead of the one the consumer of the iterator
        sees.

        NOTE: this method modifies `old_state` in-place.

        """
        new_state = {_["metadata"]["uid"]: _ for _ in manifests}
        new_keys = set(new_state)
        old_keys = set(old_state)

        # Remove the UIDs that no longer exist in the new state.
        to_remove = old_keys - new_keys
        for uid in to_remove:
            obj = old_state.pop(uid)
            await self.queue.put(WatchEvent(type="DELETED", object=obj))

        # Add the UIDs that are new.
        to_add = new_keys - old_keys
        for uid in to_add:
            obj = new_state[uid]
            old_state[uid] = obj
            await self.queue.put(WatchEvent(type="ADDED", object=obj))

        # Sanity check
        assert set(new_state) == set(old_state)

        # Emit a MODIFIED event for all objects that have changed.
        for uid, new_obj in new_state.items():
            if old_state[uid] != new_obj:
                old_state[uid] = new_obj
                await self.queue.put(WatchEvent(type="MODIFIED", object=new_obj))

    async def update_state(self, k8s_line_json: K8sLineJSON):
        """Forward K8s `k8s_line_json` to the iterator queue and track state.

        `parse_line` usually calls this to process an update event from K8s.
        Specifically, it adds the event to the iterator queue and updates the
        internal state.

        """
        meta_log = self.get_logging_metadata()

        event: EventType = k8s_line_json["type"]
        obj = k8s_line_json["object"]

        # A mutation event must carry a numeric `metadata.resourceVersion` and a
        # `metadata.uid`. Skip (rather than crash the watch on) any event that
        # is missing them (KeyError/TypeError) or whose resource version is not
        # an integer (ValueError).
        try:
            last_rv = int(obj["metadata"]["resourceVersion"])
            uid = obj["metadata"]["uid"]
        except (KeyError, TypeError, ValueError):
            meta_log["k8s_msg"] = obj
            self.logit.error(
                "K8s event missing metadata.resourceVersion/uid; skipping",
                extra=meta_log,
            )
            return

        # Track the latest resource version and enqueue the event.
        self.last_rv = last_rv

        # Sanity check.
        assert event in get_args(MutationType)
        if event == "ADDED":
            # If the UID is already in the state convert ADDED to MODIFIED.
            if uid in self.state:
                self.logit.error(f"Bug: Add existing UID <{uid}>", extra=meta_log)
                event = "MODIFIED"

            # Update the state and queue the event.
            self.state[uid] = obj
            await self.queue.put(WatchEvent(type=event, object=obj))

        elif event == "MODIFIED":
            # If the UID is not in our state convert MODIFIED to ADDED.
            if uid not in self.state:
                self.logit.error(
                    f"Bug: Modify non-existing UID <{uid}>", extra=meta_log
                )
                event = "ADDED"

            # Update the state and queue the event.
            await self.queue.put(WatchEvent(type=event, object=obj))
            self.state[uid] = obj

        else:
            # Do nothing if we do not have that UID.
            if uid not in self.state:
                self.logit.error(
                    f"Bug: Remove non-existing UID <{uid}>", extra=meta_log
                )
            else:
                del self.state[uid]
                await self.queue.put(WatchEvent(type=event, object=obj))

    async def parse_line(self, line_raw: str) -> bool:
        """Parse a single K8s watch line and forward the event to the queue.

        `read_k8s_stream` calls this once per line it reads from the stream.
        The method decodes the JSON, enqueues the resulting event and updates
        the internal state. It returns `True` when the caller must abort the
        stream and reconnect (eg a corrupt line or a non-410 K8s error) and
        `False` when the stream may continue.

        """
        meta_log = self.get_logging_metadata()

        # Return without error if K8s terminates the watch. This is
        # expected from time to time and simply means we need to restart
        # the watch from the last known resource version.
        if len(line_raw) == 0:
            self.logit.info("Connection closed", extra=meta_log)
            return False

        # K8s sends JSON encoded lines.
        try:
            line_json = json.loads(line_raw)
        except json.JSONDecodeError:
            self.logit.error("K8s sent corrupt JSON payload", extra=meta_log)
            return True

        # A valid-JSON line that is not a well-formed watch event (eg `{}`, a
        # bare value, or an error page wrapped as JSON) must abort the stream so
        # we reconnect, rather than raise an uncaught KeyError that kills the
        # watch for good.
        if (
            not isinstance(line_json, dict)
            or "type" not in line_json
            or "object" not in line_json
        ):
            meta_log["k8s_msg"] = line_json
            self.logit.error("K8s sent a malformed watch line", extra=meta_log)
            return True

        event, manifest = line_json["type"], line_json["object"]

        # A BOOKMARK only checkpoints the resource version; it is not a state
        # change, so advance `last_rv` and move on without emitting an event.
        # Ignore a bookmark that lacks a usable resource version rather than
        # crash the watch on it.
        if event == "BOOKMARK":
            try:
                self.last_rv = int(manifest["metadata"]["resourceVersion"])
            except (KeyError, TypeError, ValueError):
                pass
            return False

        # Abort if we received an ERROR or unexpected event.
        if event not in ("ADDED", "DELETED", "MODIFIED"):
            meta_log["k8s_msg"] = manifest
            self.logit.info("Received error from K8s", extra=meta_log)

            # A 410 (Gone) error is harmless and expected. We therefore
            # return without error to signal to the caller that we can (and
            # probably should) resume the watch immediately. We also need
            # to set the last seen resource version to -1 to ensure we start
            # fresh. Use `.get` because a K8s Status may omit `code` (it is
            # `omitempty`), and a missing code must not crash the watch.
            if (
                event == "ERROR"
                and isinstance(manifest, dict)
                and manifest.get("code") == 410
            ):
                self.last_rv = -1
                return False

            # Let the caller know that something unexpectedly happened. The
            # logs will contain the details.
            return True

        # Add event to queue and track the state.
        await self.update_state(cast(K8sLineJSON, line_json))
        return False

    async def read_k8s_stream(self) -> bool:
        """Connect to K8s and consume events for as long as possible.

        Return `True` if the connection dropped due to a genuine problem like a
        network error or an unhandled exception. This does not include 410
        responses or K8s closing the connection cleanly since this is expected
        to happen from time to time and simply means we should reconnect.

        """
        # Fetch the current resource list if the resource version `rv` is negative.
        if self.last_rv < 0:
            rv, err = await self.list_resource()
            if err:
                return True
            self.last_rv = rv

        # Construct the URL for a long lived watch connection.
        url = self.construct_watch_path(self.last_rv)

        # Open the long lived connection.
        try:
            async with self.k8scfg.client.stream("GET", url) as stream:
                if stream.status_code != 200:
                    meta_log = self.get_logging_metadata()
                    self.logit.warning("Cannot start watch", extra=meta_log)
                    return True

                # Feed the K8s events into our local iterator queue. A truthy
                # return from `parse_line` signals a genuine error (eg a
                # non-410 K8s error or a malformed line), so abort the stream
                # and let `background_runner` reconnect.
                async for line_raw in stream.aiter_lines():
                    if await self.parse_line(line_raw):
                        return True
        except WEB_EXCEPTIONS:
            meta_log = self.get_logging_metadata()
            self.logit.exception("Watch aborted due to a web exception", extra=meta_log)
            return True
        return False

    async def background_runner(self) -> None:
        """Watch the K8s resource indefinitely.

        This method perpetually restarts `read_k8s_stream` and does not return
        unless it receives a `CancelledError` or encounters an unhandled
        exception (ie bug).

        """
        meta_log = self.get_logging_metadata()
        try:
            # Perpetually restart the background runner.
            while True:
                self.logit.info("Reconnect", extra=meta_log)
                await self.read_k8s_stream()

                # Always wait a bit before we start/resume a watch. This is
                # purely to avoid accidental log and API spamming in case we
                # introduce a bug here (learned that the hard way).
                await asyncio.sleep(5 + random.uniform(-2, 2))
        except asyncio.CancelledError:
            self.logit.info("Background task was cancelled", extra=meta_log)
            await self.queue.put("__CANCELLED__")
        except Exception as err:
            self.logit.exception("Unhandled exception", extra=meta_log)
            await self.queue.put("__EXCEPTION__")
            raise err


def create_cluster_config(
    kubeconf: Path, context: str, disable_x509_strict: bool = False
) -> tuple[K8sConfig, bool]:
    """Build a `K8sConfig` for `context` from the `kubeconf` file.

    Load the kubeconfig, attach a ready-to-use HTTPX client and return the
    config together with an error flag. Return `(K8sConfig(), True)` if the
    kubeconfig cannot be parsed or the client cannot be created. Pass
    `disable_x509_strict=True` to relax certificate checks for old clusters.

    """
    # Parse Kubeconfig file.
    cfg, err = square.k8s.load_auto_config(kubeconf, context)
    if err:
        return K8sConfig(), True

    # Create HTTPX client. Certificate strictness stays on by default; callers
    # must opt into `disable_x509_strict=True` for old clusters that need it.
    params = ConnectionParameters(
        read=600, write=600, pool=600, disable_x509_strict=disable_x509_strict
    )
    cfg, err = square.k8s.create_httpx_client(cfg, params)
    if err:
        return K8sConfig(), True

    # Set the base URL to the K8s API server for convenience.
    cfg.client.base_url = cfg.url

    return cfg, False
