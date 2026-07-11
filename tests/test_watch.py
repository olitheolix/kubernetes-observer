import asyncio
import json
import logging
from pathlib import Path
from unittest import mock

import httpx
import pytest
import respx
import yaml
from httpx import Response
from square.dtypes import K8sConfig

import k8s_watch.watch
from k8s_watch.watch import K8sLineJSON


class TestBasic:
    def test_createClusterConfig(self, tmp_path: Path):
        # Kubeconfig does not exists.
        _, err = k8s_watch.watch.create_cluster_config(Path("/does/not/exist"), "")
        assert err

        # Valid Kubeconfig.
        kubeconf = Path("tests/support/valid_kubeconf.yaml")
        _, err = k8s_watch.watch.create_cluster_config(kubeconf, "kind-kind")
        assert not err

        # Corrupt the valid Kubeconfig to force the SSL error.
        kubeconf_dict = yaml.safe_load(kubeconf.read_text())
        user = kubeconf_dict["users"][0]["user"]
        user["client-key-data"] = ""
        kubeconf2 = tmp_path / "kubeconf.yaml"
        kubeconf2.write_text(yaml.dump(kubeconf_dict))

        _, err = k8s_watch.watch.create_cluster_config(kubeconf2, "kind-kind")
        assert err


class TestWatchMockedBackgroundTask:
    @pytest.fixture(autouse=True)
    def setup(self):
        """Replace the list of tasks with a list of a single mock."""
        with mock.patch.object(k8s_watch.watch.WatchResource, "start_tasks") as m:
            # Model a real `asyncio.Task`: awaitable, but with a *synchronous*
            # `.result()`. A bare `AsyncMock` makes `.result()` return an
            # unawaited coroutine, which triggers a `RuntimeWarning`.
            m.return_value = [mock.AsyncMock(spec=asyncio.Task)]
            yield

    async def test_ctor(self, k8scfg: K8sConfig):
        path = "/api/crt/v1/namespaces"

        # Default values.
        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert watch.k8scfg == k8scfg
        assert watch.last_rv == -1
        assert watch.queue.qsize() == 0
        assert watch.list_path == path
        assert (
            watch.watch_path == "/api/crt/v1/namespaces?watch=true&timeoutSeconds=300"
        )
        assert watch.logit == logging.getLogger("Watch")
        assert len(watch.tasks) == 1

        # Custom values.
        custom_logger = logging.getLogger("default")
        watch = k8s_watch.watch.WatchResource(
            k8scfg, path, rv=10, timeout=20, logger=custom_logger
        )
        assert watch.k8scfg == k8scfg
        assert watch.last_rv == 10
        assert watch.queue.qsize() == 0
        assert watch.list_path == path
        assert watch.watch_path == "/api/crt/v1/namespaces?watch=true&timeoutSeconds=20"
        assert watch.logit == custom_logger
        assert len(watch.tasks) == 1

    async def test_get_logging_metadata(self, k8scfg):
        """Basic test to validate the logging metadata."""
        path = "/api/crt/v1/namespaces"

        # Session without explicit host.
        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        ret = watch.get_logging_metadata()
        assert ret == {
            "component": "k8s-watch",
            "path": path,
            "host": "https:",
        }

        # Session with explicit host.
        k8scfg.client.base_url = "http://10.1.2.3:8080"
        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        ret = watch.get_logging_metadata()
        assert ret == {
            "component": "k8s-watch",
            "path": path,
            "host": "http://10.1.2.3:8080",
        }

    async def test_context_manager(self, k8scfg: K8sConfig):
        """Context manager must cancel *and await* all tasks."""
        path = "/api/crt/v1/namespaces"

        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        # Swap the mocked task for a real, long-running one so we can assert the
        # context manager both cancels it and waits for it to unwind (otherwise
        # teardown races the caller closing the HTTP client).
        async def _forever():
            await asyncio.sleep(3600)

        real_task = asyncio.ensure_future(_forever())
        watch.tasks = [real_task]

        async with watch:
            assert not real_task.done()
        assert real_task.cancelled()

    async def test_WatchResource_iterator_basic(self, k8scfg: K8sConfig):
        """The class must yield the content of the queue."""
        path = "/api/crt/v1/namespaces"

        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        # Pretend K8s sent us one line before the task was cancelled.
        await watch.queue.put("k8s-line-1")
        await watch.queue.put("__CANCELLED__")
        data = [_ async for _ in watch]
        assert data == ["k8s-line-1"]

        # Pretend K8s sent us one line before the task raised an unhandled exception.
        await watch.queue.put("k8s-line-2")
        await watch.queue.put("__EXCEPTION__")
        data = [_ async for _ in watch]
        assert data == ["k8s-line-2"]

    async def test_list_resources_ok(self, k8scfg: K8sConfig):
        """Must return the correct resource version from a LIST operation."""
        # Important: K8s returns the resourceVersion as a string, not an integer.
        uid = "50"
        obj = {"metadata": {"uid": uid}}
        manifest = {"metadata": {"resourceVersion": "5"}, "items": [obj]}
        path = "/api/crt/v1/namespaces"

        # Mock the K8s request to return our dummy manifests.
        m_http = respx.get(path)
        m_http.return_value = Response(200, json=manifest)

        # Function must return the resource version as an *integer*. This
        # is because K8s encodes the resource version as a string.
        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert watch.state == {}
        ret = await watch.list_resource()
        assert watch.state == {uid: obj}
        assert watch.queue.qsize() == 1
        assert ret == (5, False)

    async def test_list_resources_err(self, k8scfg: K8sConfig):
        """Must gracefully handle errors during the LIST operation."""
        path = "/does/not/exist"

        # Pretend the server responds with 404.
        m_http = respx.get(path)
        m_http.return_value = Response(404, json={})

        # The `list_resource` method must return with an error.
        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert await watch.list_resource() == (-1, True)

    async def test_update_state_inconsistent(self, k8scfg: K8sConfig):
        """`update_state` must be able to cope with non-existing UIDs.

        Here we ask the function to modify and remove two resources that are
        not tracked as part of the state. While this is almost certainly a bug
        the function must still be able to function.

        """
        path = "/api/crt/v1/namespaces"
        rv1, rv2 = 30, 40
        uid1, uid2 = "1", "2"

        # Simulate K8s sending three lines followed by an empty one to signify
        # that the connection is now closed.
        obj1 = {"metadata": {"resourceVersion": str(rv1), "uid": uid1}}
        obj2 = {"metadata": {"resourceVersion": str(rv2), "uid": uid2}}
        line_mod_1: K8sLineJSON = {"type": "MODIFIED", "object": obj1}
        line_del_2: K8sLineJSON = {"type": "DELETED", "object": obj2}
        line_add_1: K8sLineJSON = {"type": "ADDED", "object": obj1}

        # Create the `WatchResource` instance and ensure it reads the lines
        # and puts them into the queue before returning successfully.
        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert watch.state == {}
        assert watch.last_rv == -1
        assert watch.queue.qsize() == 0

        # Modify the non-existing `obj1`. The function must add the
        # corresponding UID to the state and emit an ADDED event because as
        # far as this class is concerned the resource is new.
        await watch.update_state(line_mod_1)
        assert watch.last_rv == rv1
        assert watch.state == {uid1: obj1}
        assert watch.queue.qsize() == 1
        event = await watch.queue.get()
        assert event.type == "ADDED"

        # Delete the non-existing `obj1`. The function must silently ignore
        # the event and not queue any events.
        await watch.update_state(line_del_2)
        assert watch.last_rv == rv2
        assert watch.state == {uid1: obj1}
        assert watch.queue.qsize() == 0

        # Add the already `obj1` a second time. The function must treat
        # this as a MODIFIED event.
        await watch.update_state(line_add_1)
        assert watch.last_rv == rv1
        assert watch.state == {uid1: obj1}
        assert watch.queue.qsize() == 1
        event = await watch.queue.get()
        assert event.type == "MODIFIED"

    async def test_update_state(self, k8scfg: K8sConfig):
        """Pass ADDED, MODIFIED and DELETED events and verify the state."""
        path = "/api/crt/v1/namespaces"
        uid1 = "1"

        # Simulate K8s sending three lines followed by an empty one to signify
        # that the connection is now closed.
        line_add: K8sLineJSON = {"object": {"metadata": {"uid": uid1}}, "type": "ADDED"}
        line_mod: K8sLineJSON = {
            "object": {"metadata": {"uid": uid1}},
            "type": "MODIFIED",
        }
        line_del: K8sLineJSON = {
            "object": {"metadata": {"uid": uid1}},
            "type": "DELETED",
        }
        line_add["object"]["metadata"]["resourceVersion"] = "1"
        line_mod["object"]["metadata"]["resourceVersion"] = "2"
        line_del["object"]["metadata"]["resourceVersion"] = "3"

        # Create the `WatchResource` instance and ensure it reads the lines
        # and puts them into the queue before returning successfully.
        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert watch.state == {}
        assert watch.last_rv == -1
        assert watch.queue.qsize() == 0

        # Add a new object.
        await watch.update_state(line_add)
        assert watch.last_rv == 1
        assert watch.state == {uid1: line_add["object"]}
        assert watch.queue.qsize() == 1
        event = await watch.queue.get()
        assert event.type == "ADDED"

        # Modify the existing object.
        await watch.update_state(line_mod)
        assert watch.last_rv == 2
        assert watch.state == {uid1: line_mod["object"]}
        assert watch.queue.qsize() == 1
        event = await watch.queue.get()
        assert event.type == "MODIFIED"

        # Delete the object.
        await watch.update_state(line_del)
        assert watch.last_rv == 3
        assert watch.state == {}
        assert watch.queue.qsize() == 1
        event = await watch.queue.get()
        assert event.type == "DELETED"

    async def test_parse_line_ok(self, k8scfg: K8sConfig):
        """Use valid K8s events to verify the line processing."""
        path = "/api/crt/v1/namespaces"
        rv1, uid1 = 30, "1"

        # Simulate a valid payload from a K8s watch stream.
        obj1 = {"metadata": {"resourceVersion": str(rv1), "uid": uid1}}
        line = json.dumps({"type": "ADDED", "object": obj1})

        # Setup Watch.
        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert watch.state == {}
        assert watch.last_rv == -1

        # Function must do nothing if the line is empty. Empty lines are
        # harmless and signify that K8s has closed the connection.
        assert await watch.parse_line("") is False
        assert watch.last_rv == -1

        # Use a valid K8s event and verify that the function updated the
        # value of `last_rv`, the internal state and queued an ADDED event.
        assert await watch.parse_line(line) is False
        assert watch.last_rv == rv1
        assert watch.state == {uid1: obj1}
        assert watch.queue.qsize() == 1
        ret = await watch.queue.get()
        assert ret.type == "ADDED"

    @pytest.mark.parametrize("is_410", [True, False])
    async def test_parse_line_k8s_err(self, is_410: bool, k8scfg: K8sConfig):
        """Must be able to handle error manifests.

        K8s will send ERROR events from time to time. This is expected and
        the function must be able to process them and return an error. The only
        exception is a 410 error since that one is expected and does not
        constitute an error for us because all it means is that we should
        resume the watch immediately.

        """
        path = "/api/crt/v1/namespaces"
        line = json.dumps(
            {
                "type": "ERROR",
                "object": {
                    "apiVersion": "v1",
                    "code": 410 if is_410 else 420,
                    "kind": "Status",
                    "message": "too old resource version: 11498 (39652)",
                    "metadata": {},
                    "reason": "Expired",
                    "status": "Failure",
                },
            }
        )

        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert watch.last_rv == -1

        # All errors except 410 must propagate to the caller.
        # 410 errors must not propagate to the caller but the resource version must have been reset to -1.
        watch.last_rv = 123
        err = await watch.parse_line(line)
        assert err is (False if is_410 else True)
        assert watch.last_rv == (-1 if is_410 else 123)
        assert watch.queue.qsize() == 0

    async def test_parse_line_json_err(self, k8scfg: K8sConfig):
        """Gracefully abort if we receive a corrupt JSON line."""
        path = "/api/crt/v1/namespaces"

        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert watch.last_rv == -1

        # Must return with an error and not do anything else.
        assert await watch.parse_line("{invalid json]") is True
        assert watch.last_rv == -1
        assert watch.state == {}
        assert watch.queue.qsize() == 0

    @pytest.mark.parametrize("line", ["{}", "null", "42", '{"type": "ADDED"}'])
    async def test_parse_line_malformed(self, line: str, k8scfg: K8sConfig):
        """Valid JSON that is not a well-formed watch line must not crash.

        A `KeyError` here used to propagate out of the background task and kill
        the watch permanently. It must instead be reported as an error so the
        caller reconnects.

        """
        path = "/api/crt/v1/namespaces"
        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        assert await watch.parse_line(line) is True
        assert watch.last_rv == -1
        assert watch.state == {}
        assert watch.queue.qsize() == 0

    async def test_parse_line_error_without_code(self, k8scfg: K8sConfig):
        """An ERROR Status without a `code` must not crash the watch.

        K8s omits `code` from a Status when it is unset (`omitempty`), so the
        old unguarded `manifest["code"]` access could raise `KeyError`.

        """
        path = "/api/crt/v1/namespaces"
        line = json.dumps({"type": "ERROR", "object": {"kind": "Status"}})

        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        watch.last_rv = 123

        # Missing code is not a 410, so it must propagate as an error and leave
        # the resource version untouched.
        assert await watch.parse_line(line) is True
        assert watch.last_rv == 123
        assert watch.queue.qsize() == 0

    async def test_parse_line_bookmark(self, k8scfg: K8sConfig):
        """A BOOKMARK must advance `last_rv` without emitting an event."""
        path = "/api/crt/v1/namespaces"
        obj = {"metadata": {"resourceVersion": "77"}}
        line = json.dumps({"type": "BOOKMARK", "object": obj})

        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        assert await watch.parse_line(line) is False
        assert watch.last_rv == 77
        assert watch.state == {}
        assert watch.queue.qsize() == 0

    async def test_parse_line_bookmark_non_numeric(self, k8scfg: K8sConfig):
        """A BOOKMARK with a non-numeric resourceVersion must be ignored."""
        path = "/api/crt/v1/namespaces"
        obj = {"metadata": {"resourceVersion": "not-a-number"}}
        line = json.dumps({"type": "BOOKMARK", "object": obj})

        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        watch.last_rv = 55

        # `last_rv` must stay untouched rather than crash on the bad value.
        assert await watch.parse_line(line) is False
        assert watch.last_rv == 55
        assert watch.state == {}
        assert watch.queue.qsize() == 0

    async def test_update_state_missing_metadata(self, k8scfg: K8sConfig):
        """`update_state` must skip events without resourceVersion/uid."""
        path = "/api/crt/v1/namespaces"
        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        # Object without metadata at all.
        line: K8sLineJSON = {"type": "ADDED", "object": {}}
        await watch.update_state(line)
        assert watch.last_rv == -1
        assert watch.state == {}
        assert watch.queue.qsize() == 0

        # Object with metadata but no uid.
        line = {"type": "ADDED", "object": {"metadata": {"resourceVersion": "5"}}}
        await watch.update_state(line)
        assert watch.last_rv == -1
        assert watch.state == {}
        assert watch.queue.qsize() == 0

    async def test_list_resource_malformed_body(self, k8scfg: K8sConfig):
        """A 200 body that is not a resource collection must error, not crash."""
        path = "/api/crt/v1/namespaces"

        # 200 OK but the body has neither `items` nor `metadata.resourceVersion`.
        m_http = respx.get(path)
        m_http.return_value = Response(200, json={"kind": "Namespace"})

        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert await watch.list_resource() == (-1, True)
        assert watch.state == {}
        assert watch.queue.qsize() == 0

    @mock.patch.object(k8s_watch.watch.WatchResource, "list_resource")
    async def test_read_k8s_stream_parse_error_aborts(self, m_list, k8scfg: K8sConfig):
        """A genuine `parse_line` error must abort the stream and signal upward."""
        path, rv = "/api/crt/v1/namespaces", 10

        watch = k8s_watch.watch.WatchResource(k8scfg, path, rv=rv)
        watch.last_rv = rv
        m_list.return_value = (rv, False)

        m_http = respx.get(watch.construct_watch_path(rv))
        m_http.return_value = Response(200, text="line1\nline2")

        with mock.patch.object(watch, "parse_line", mock.AsyncMock()) as m_parse:
            # First line reports a genuine error.
            m_parse.side_effect = [True, False]
            assert await watch.read_k8s_stream() is True

        # Must have aborted after the first (erroring) line.
        assert m_parse.call_count == 1

    @pytest.mark.parametrize("status", [200, 404])
    @pytest.mark.parametrize("initial_rv", [10, -10])
    @mock.patch.object(k8s_watch.watch.WatchResource, "parse_line")
    @mock.patch.object(k8s_watch.watch.WatchResource, "list_resource")
    async def test_read_k8s_stream_restart(
        self, m_list, m_parse, initial_rv: int, status: int, k8scfg: K8sConfig
    ):
        """Simulate watch restart because resource version was negative."""
        path, rv = "/api/crt/v1/namespaces", 10

        watch = k8s_watch.watch.WatchResource(k8scfg, path, rv=rv)
        watch.last_rv = initial_rv
        m_list.return_value = (10, False)

        # `parse_line` reports "no error" so the stream is consumed to the end.
        m_parse.return_value = False

        # Setup a mock to return a multi-line text response.
        m_http = respx.get(watch.construct_watch_path(rv))
        m_http.return_value = Response(status, text="line1\nline2")

        # Consume the stream and verify the function processes the events.
        ret = await watch.read_k8s_stream()
        if status == 200:
            # Must have passed both messages to `parse_line`.
            assert ret is False and m_parse.call_count == 2
        else:
            # Must return with an error not have parsed any messages.
            assert ret is True and m_parse.call_count == 0

    @mock.patch.object(k8s_watch.watch.WatchResource, "parse_line")
    @mock.patch.object(k8s_watch.watch.WatchResource, "list_resource")
    async def test_read_k8s_stream_restart_bugfix(
        self, m_list, m_parse, k8scfg: K8sConfig
    ):
        """A web exception during the stream must abort with an error."""
        path, rv = "/api/crt/v1/namespaces", 10

        watch = k8s_watch.watch.WatchResource(k8scfg, path, rv=rv)
        watch.last_rv = 10
        m_list.return_value = (10, False)

        # Setup a mock to return a multi-line text response.
        m_http = respx.get(watch.construct_watch_path(rv))
        m_http.side_effect = httpx.RequestError

        # Consume the stream and verify the function processes the events.
        err = await watch.read_k8s_stream()
        assert err

    async def test_read_k8s_stream_list_error(self, k8scfg: K8sConfig):
        """Gracefully abort if LIST operations fails."""
        path = "/api/crt/v1/namespaces"

        m_http = respx.get(path)
        m_http.return_value = Response(404, json={})

        # Create watch.
        watch = k8s_watch.watch.WatchResource(k8scfg, path)
        assert watch.last_rv == -1

        # Stream reader must return with an error and not update the latest
        # resource version.
        assert await watch.read_k8s_stream() is True
        assert watch.last_rv == -1
        assert watch.queue.qsize() == 0

    @mock.patch.object(k8s_watch.watch.random, "uniform")
    @mock.patch.object(k8s_watch.watch.WatchResource, "read_k8s_stream")
    @mock.patch.object(k8s_watch.watch.asyncio, "sleep")
    async def test_background_runner_loop(
        self, m_sleep, m_bgs, m_rand, k8scfg: K8sConfig
    ):
        """Runner must restart the `read_k8s_stream`.

        If `read_k8s_stream` returns an error it must wait 5s (plus jitter)
        before it tries again.

        """
        m_rand.return_value = 1.5
        path = "/api/crt/v1/namespaces"

        # The background function must raise the unhandled exception but only
        # after it queued the __CANCELLED__ message.
        m_bgs.side_effect = [False, True, asyncio.CancelledError]

        watch = k8s_watch.watch.WatchResource(k8scfg, path, rv=-1)
        await watch.background_runner()

        assert m_sleep.call_count == m_rand.call_count == 2
        m_sleep.assert_called_with(5 + 1.5)
        m_rand.assert_called_with(-2, 2)

    @mock.patch.object(k8s_watch.watch.asyncio, "sleep")
    @mock.patch.object(k8s_watch.watch.WatchResource, "read_k8s_stream")
    async def test_background_runner_cancelled(self, m_bgs, m_sleep, k8scfg: K8sConfig):
        """Runner task must emit __CANCELLED__ and shut down cleanly."""
        path = "/api/crt/v1/namespaces"

        # The background function must raise the unhandled exception but only
        # after it queued the __CANCELLED__ message.
        m_bgs.side_effect = asyncio.CancelledError
        m_sleep.return_value = None

        watch = k8s_watch.watch.WatchResource(k8scfg, path, rv=-1)
        await watch.background_runner()
        assert await watch.queue.get() == "__CANCELLED__"
        assert watch.queue.qsize() == 0

    @mock.patch.object(k8s_watch.watch.asyncio, "sleep")
    @mock.patch.object(k8s_watch.watch.WatchResource, "read_k8s_stream")
    async def test_background_runner_unhandled_exception(
        self, m_bgs, m_sleep, k8scfg: K8sConfig
    ):
        """Runner task must emit __EXCEPTION__ and shut down cleanly."""
        path = "/api/crt/v1/namespaces"

        # Pretend the background task aborted with an exception.
        m_bgs.side_effect = ValueError
        m_sleep.return_value = None

        # The background function must raise the unhandled exception but only
        # after it queued the __EXCEPTION__ message.
        watch = k8s_watch.watch.WatchResource(k8scfg, path, rv=-1)

        try:
            await watch.background_runner()
            assert False
        except ValueError:
            pass
        assert await watch.queue.get() == "__EXCEPTION__"
        assert watch.queue.qsize() == 0

    async def test_reset_state_no_op(self, k8scfg: K8sConfig):
        """The old state matches the new state."""
        path = "/api/crt/v1/namespaces"
        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        # No existing state and no new manifests. This must do nothing.
        state: dict[str, dict] = {}
        await watch.reset_state([], state)
        assert state == {} and watch.queue.qsize() == 0

        # Existing state matches the new manifest.
        obj = {"metadata": {"uid": "1"}}
        state = {"1": obj}
        await watch.reset_state([obj], state)
        assert state == {"1": obj} and watch.queue.qsize() == 0

    async def test_reset_state_add(self, k8scfg: K8sConfig):
        """Add new objects to the state."""
        path = "/api/crt/v1/namespaces"

        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        # Must add one document to the state and fake the corresponding event.
        state: dict[str, dict] = {}
        obj = {"metadata": {"uid": "1"}}
        await watch.reset_state([obj], state)
        assert state == {"1": obj}
        assert watch.queue.qsize() == 1
        assert await watch.queue.get() == k8s_watch.watch.WatchEvent(
            type="ADDED", object=obj
        )

    async def test_reset_state_modified(self, k8scfg: K8sConfig):
        """Patch existing objects in the state."""
        path = "/api/crt/v1/namespaces"

        # Same object but different content. Function must update the state
        # and emit the corresponding MODIFIED event.
        obj1_a = {"metadata": {"uid": "1", "foo": "bar_a"}}
        obj1_b = {"metadata": {"uid": "1", "foo": "bar_b"}}

        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        # Same object but different content. Function must update the state
        # and emit the corresponding MODIFIED event.
        state = {"1": obj1_a}
        await watch.reset_state([obj1_b], state)
        assert state == {"1": obj1_b}

        assert watch.queue.qsize() == 1
        assert await watch.queue.get() == k8s_watch.watch.WatchEvent(
            type="MODIFIED", object=obj1_b
        )

    async def test_reset_state_delete(self, k8scfg: K8sConfig):
        """Remove objects from the state."""
        path = "/api/crt/v1/namespaces"

        # Same object but different content. Function must update the state
        # and emit the corresponding DELETED event.
        obj1_a = {"metadata": {"uid": "1", "foo": "bar_a"}}
        obj2_a = {"metadata": {"uid": "2", "foo": "bar_a"}}

        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        state = {"1": obj1_a, "2": obj2_a}
        await watch.reset_state([obj1_a], state)
        assert state == {"1": obj1_a}
        assert watch.queue.qsize() == 1
        assert await watch.queue.get() == k8s_watch.watch.WatchEvent(
            type="DELETED", object=obj2_a
        )

    async def test_reset_state_mixed(self, k8scfg: K8sConfig):
        """Add, remove and modify objects in the state."""
        path = "/api/crt/v1/namespaces"

        # Dummy manifests.
        obj1_a = {"metadata": {"uid": "1", "foo": "bar_a"}}
        obj2_a = {"metadata": {"uid": "2", "foo": "bar_a"}}
        obj2_b = {"metadata": {"uid": "2", "foo": "bar_b"}}
        obj3_a = {"metadata": {"uid": "3", "foo": "bar_a"}}
        obj4_b = {"metadata": {"uid": "4", "foo": "bar_b"}}

        watch = k8s_watch.watch.WatchResource(k8scfg, path)

        # Current state knows of three objects but the new manifests add,
        # remove and modify one.
        state = {"1": obj1_a, "2": obj2_b, "3": obj3_a}
        await watch.reset_state([obj1_a, obj2_a, obj4_b], state)
        assert state == {"1": obj1_a, "2": obj2_a, "4": obj4_b}

        assert watch.queue.qsize() == 3
        assert await watch.queue.get() == k8s_watch.watch.WatchEvent(
            type="DELETED", object=obj3_a
        )
        assert await watch.queue.get() == k8s_watch.watch.WatchEvent(
            type="ADDED", object=obj4_b
        )
        assert await watch.queue.get() == k8s_watch.watch.WatchEvent(
            type="MODIFIED", object=obj2_a
        )


class TestWatchWithBackgroundTask:
    @mock.patch.object(k8s_watch.watch.asyncio, "sleep")
    @mock.patch.object(k8s_watch.watch.WatchResource, "read_k8s_stream")
    async def test_background_runner(self, m_bgs, m_sleep, k8scfg: K8sConfig):
        """Ensure that `WatchResource` actually starts the runner.

        To verify that, we will instantiate `WatchResource` with a mocked
        `read_k8s_stream` that immediately raises a `CancelledError`.
        If everything works as expected then the iterator must return without
        yielding any results.

        """
        path = "/api/crt/v1/namespaces"

        m_bgs.side_effect = asyncio.CancelledError
        m_sleep.return_value = None

        watch = k8s_watch.watch.WatchResource(k8scfg, path, rv=-1)
        results = [_ async for _ in watch]
        assert len(results) == 0
