"""Forward router, multi-backend (SEP §3, draft §5.2, §6.3).

Aggregates several backends behind one client-facing surface. On list methods
it fans out to every backend and namespaces the results; on ``tools/call`` it
reverse-resolves the namespaced name to exactly one backend and forwards the
call with the original name. Names that cannot be resolved are rejected rather
than forwarded. Collisions across backends are resolved by the prefix strategy:
identical original names get distinct backend prefixes.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Awaitable, Callable

import json

from . import auth, capability, collision, errors, filters, jsonrpc, namespace, observability, pool, routing, schema, server, signing, subscriptions, tasks, variants
from .cache import ListCache
from .config import Namespacing
from .errors import UNAUTHORIZED
from .handler import Registry
from .forward import HandshakeError, PROXY_PROTOCOL_VERSION, PROXY_SERVER_INFO
from .jsonrpc import INTERNAL_ERROR, INVALID_PARAMS, INVALID_REQUEST, METHOD_NOT_FOUND
from .resilience import SERVER_NOT_AVAILABLE, CircuitBreaker, partial_meta
from .transport.base import Transport

ServerMessageSink = Callable[[str, jsonrpc.Message], Awaitable[None]]

# Backend notifications that invalidate a cached list for that backend (SEP §6.2).
_LIST_CHANGED_METHODS = frozenset(
    {
        "notifications/tools/list_changed",
        "notifications/prompts/list_changed",
        "notifications/resources/list_changed",
    }
)


class Capability:
    """One namespaced capability kind: how it lists, calls, and is named."""

    def __init__(self, list_method, call_method, collection, field, prefix, split):
        self.list_method = list_method
        self.call_method = call_method
        self.collection = collection
        self.field = field
        self.prefix = prefix
        self.split = split


_CAPABILITIES = [
    Capability("tools/list", "tools/call", "tools", "name", namespace.prefix, namespace.split),
    Capability("prompts/list", "prompts/get", "prompts", "name", namespace.prefix, namespace.split),
    Capability("resources/list", "resources/read", "resources", "uri", namespace.prefix_uri, namespace.split_uri),
]
_LIST_METHODS = {cap.list_method: cap for cap in _CAPABILITIES}
_CALL_METHODS = {cap.call_method: cap for cap in _CAPABILITIES}
_TOOLS_CAPABILITY = _CAPABILITIES[0]
# Resource subscriptions (σ4) reverse-resolve their URI exactly as resources/read
# does, so they reuse the resources capability's namespacing.
_RESOURCES_CAPABILITY = _CAPABILITIES[2]
# Stateless discovery (SEP §2.1): the intermediary MUST answer server/discover by
# composing results from all healthy backends. It aggregates the same tool
# surface as tools/list, so it reuses that capability's fan-out.
_DISCOVER_METHOD = "server/discover"


def _params(message: jsonrpc.Message) -> dict:
    """A request's ``params`` as a dict. A spec-valid but non-object ``params``
    (JSON ``null``, an array, or a scalar from a hostile client) is treated as
    empty rather than crashing the connection's serve loop with ``AttributeError``.
    Matches the Rust arm, whose ``params.get(...)`` yields ``None`` on a non-object."""
    params = message.get("params")
    return params if isinstance(params, dict) else {}


class Backend:
    """One backend connection with a demuxing read loop.

    A background reader matches responses to pending requests by id. Anything
    else the backend sends (a server-initiated request such as sampling, or a
    notification) is handed to the sink set by :meth:`start`.
    """

    def __init__(
        self,
        id: str,
        transport: Transport,
        breaker: CircuitBreaker | None = None,
        request_timeout: float | None = None,
        keywords: list[str] | None = None,
        token: str | None = None,
    ) -> None:
        if not namespace.valid_backend_id(id):
            raise ValueError(f"invalid backend id: {id!r}")
        self.id = id
        # Keyword hints (SEP-2614) used to pre-select this backend on a filtered
        # list; empty means the backend is always queried.
        self.keywords = keywords or []
        # The backend's own credential, injected into forwarded requests; the
        # client's credential is never forwarded (SEP §13.1, confused deputy).
        self.token = token
        self._transport = transport
        self._next_id = 0
        self.capabilities: jsonrpc.Message = {}
        self.server_info: jsonrpc.Message | None = None
        self._pending: dict[object, asyncio.Future] = {}
        self._on_server: ServerMessageSink | None = None
        self._reader: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()
        self.breaker = breaker
        self._timeout = request_timeout

    def available(self) -> bool:
        """Whether the breaker permits traffic (always true with no breaker)."""
        return self.breaker is None or self.breaker.allow()

    def start(self, on_server: ServerMessageSink) -> None:
        self._on_server = on_server
        self._reader = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        try:
            while True:
                raw = await self._transport.receive()
                if raw is None:
                    break
                message = jsonrpc.decode(raw)
                mid = message.get("id")
                future = self._pending.pop(mid, None) if mid is not None else None
                if future is not None:
                    if not future.done():
                        future.set_result(message)
                elif self._on_server is not None:
                    await self._on_server(self.id, message)
        finally:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(HandshakeError(f"backend {self.id} closed"))
            self._pending.clear()

    async def handshake(self) -> None:
        init = await self.request(
            "initialize",
            {
                "protocolVersion": PROXY_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": PROXY_SERVER_INFO,
            },
        )
        result = init.get("result", {})
        self.capabilities = result.get("capabilities", {})
        self.server_info = result.get("serverInfo")
        await self._transport.send(
            jsonrpc.encode(jsonrpc.notification("notifications/initialized"))
        )

    async def request(self, method: str, params: jsonrpc.Message) -> jsonrpc.Message:
        # Confused-deputy: strip any client credential and inject the backend's
        # own before forwarding (SEP §13.1). Skip when there is nothing to do so
        # an empty _meta is not added to every request.
        meta = params.get("_meta")
        if self.token is not None or (isinstance(meta, dict) and auth.AUTHORIZATION_META_KEY in meta):
            params = dict(params)
            params["_meta"] = auth.forward_meta(meta or {}, self.token)
        self._next_id += 1
        mid = f"{self.id}-{self._next_id}"
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[mid] = future
        # Serialize byte writes: the route loop and the health pinger may both
        # send to this backend at once.
        async with self._send_lock:
            await self._transport.send(jsonrpc.encode(jsonrpc.request(mid, method, params)))
        try:
            if self._timeout is not None:
                result = await asyncio.wait_for(future, self._timeout)
            else:
                result = await future
        except (asyncio.TimeoutError, Exception):
            self._pending.pop(mid, None)
            if self.breaker is not None:
                self.breaker.record_failure()
            raise
        if self.breaker is not None:
            self.breaker.record_success()
        return result

    async def send_message(self, message: jsonrpc.Message) -> None:
        """Send a raw message to the backend without tracking a response.

        Used to relay a client's reply to a backend-initiated request and to
        forward client notifications (SEP §5.1); neither expects a reply the
        proxy correlates.
        """
        async with self._send_lock:
            await self._transport.send(jsonrpc.encode(message))

    async def close(self) -> None:
        if self._reader is not None:
            self._reader.cancel()
        await self._transport.send_eof()


class ForwardRouter:
    def __init__(
        self,
        client: Transport,
        backends: list[Backend],
        on_server_message: ServerMessageSink | None = None,
        health_interval: float | None = None,
        trace: bool = True,
        disclose: bool = False,
        disclose_threshold: int = capability.DEFAULT_TOOL_THRESHOLD,
        cache: ListCache | None = None,
        principal: str | None = None,
        namespacing: Namespacing | None = None,
        registry: Registry | None = None,
        issuer: str | None = None,
        audience: str | None = None,
        audit: signing.AuditLog | None = None,
        filter_chain: filters.FilterChain | None = None,
    ) -> None:
        self._client = client
        self._backends = {backend.id: backend for backend in backends}
        # Expected token claims (SEP-2468). When set, the client's initialize
        # must carry matching iss/aud claims or the handshake is rejected.
        self._issuer = issuer
        self._audience = audience
        # Local handlers that originate responses (draft §5.3/§5.7). Their tools
        # merge into tools/list under their reserved ids, and a tools/call whose
        # prefix names a handler is served locally rather than routed.
        self._registry = registry or Registry()
        # Collision resolution strategy (SEP §3.4). Default is prefix, which the
        # labeling already applies; priority additionally drops lower-priority
        # duplicates of the same original tool name.
        self._namespacing = namespacing or Namespacing()
        # Optional shared list cache (SEP §6). When several client connections
        # share one cache, repeated list fetches collapse to O(backends). The
        # principal scopes private entries so they never cross users.
        self._cache = cache
        self._principal = principal
        # Hop tracing on forwarded messages (SEP §7.1). Default on: the proxy is
        # an intermediary and MUST record its hop.
        self._trace = trace
        # Where backend-initiated messages go. Default is the client transport
        # (stdio); an HTTP server passes a sink that routes them to its SSE
        # stream instead, so the client transport carries only responses.
        self._on_server_message = on_server_message
        # Correlation for backend-initiated requests (sampling, elicitation).
        # A backend's own request id may collide with another backend's, so the
        # proxy mints a unique client-facing id and remembers where to route the
        # client's reply back to (SEP §5.1).
        self._server_requests: dict[str, tuple[str, object]] = {}
        self._server_request_seq = 0
        # Reverse map for the passthrough collision strategy (SEP §3.4): exposed
        # tool name -> (backend id, original name), populated as tools are listed
        # so a later tools/call by the original name still resolves to one backend.
        self._reverse: dict[str, tuple[str, str]] = {}
        # Composed server variants (SEP-2053), filled at handshake by intersecting
        # the backends' offerings. The first is the default; empty means no backend
        # supports variants, so a selected variant is rejected.
        self._variants: list[str] = []
        # Optional accountability log (corpus SEP-2828/2787). When set, each routed
        # call appends a pre-call attestation and a post-call outcome, best-effort:
        # any failure is swallowed so audit never blocks or fails traffic.
        self._audit = audit
        # Extension filter chain run on the client request before it is routed
        # (ε0). None means no filters, so the seam costs nothing.
        self._filter_chain = filter_chain
        # Cache directives (ttlMs, cacheScope) the server attaches to its served
        # list results (σ0). None means no directives are added.
        self._list_directives: tuple[int, str] | None = None
        # Validate a local handler's tools/call arguments against its inputSchema
        # and its result against outputSchema (σ1). A server-role act,
        # off by default so existing flows stay byte-identical.
        self._validate_schemas = False
        # Worker pool for server-originated calls (σ2). Off by default,
        # so the route loop stays serial and existing flows byte-identical. When on,
        # a tools/call resolving to a local handler runs as a bounded, cancellable
        # concurrent task; the InFlight registry is the shared bookkeeping.
        self._pool_enabled = False
        self._pool_cap = 0
        self._pool_idle_ms = 0
        self._pool_sem: asyncio.Semaphore | None = None
        self._inflight = pool.InFlight()
        self._pool_tasks: dict = {}  # client request id -> running asyncio.Task
        self._pool_token_to_call: dict = {}  # progressToken -> in-flight call id
        # Server-side task origination (σ3). Off by default; when on, a
        # task-augmented local call returns a working handle and runs in the
        # background, and the store serves the later tasks/get and tasks/cancel.
        self._server_tasks = False
        self._tasks_store = tasks.ServerTasks()
        self._task_handles: dict = {}  # taskId -> running asyncio.Task
        # Server-side resource subscriptions (σ4). Off by default; when
        # on, a subscribe whose URI does not resolve to a backend is registered in
        # this per-connection registry, and publish_resource_update fans out
        # notifications/resources/updated only to the subscribed URIs. Proxy-side
        # routing of subscribe/unsubscribe to a backend needs no toggle.
        self._subscriptions_enabled = False
        self._subscriptions = subscriptions.Subscriptions()
        # Deterministic output/memory bound (σ5): a server-originated
        # result whose encoded form exceeds this cap is rejected with a server-class
        # error rather than emitted. Defaults to the frame ceiling, so it never
        # trips in normal use and existing flows stay byte-identical.
        self._output_limit = server.MAX_OUTPUT_BYTES
        # Graceful drain window in ms (σ5): on shutdown, in-flight server-originated
        # work is given this long to finish before it is cancelled. 0 means cancel
        # immediately, the σ2/σ3 behavior, so existing flows are unchanged.
        self._drain_ms = 0
        self._client_lock = asyncio.Lock()
        # Resilience is active only when at least one backend has a breaker.
        # With none, routing keeps its strict behavior and failures propagate.
        self._resilient = any(backend.breaker is not None for backend in backends)
        self._surface = frozenset(self._backends)
        # A single-backend intermediary MUST NOT modify names (SEP §5.3): with
        # one backend there is nothing to disambiguate, so names pass through.
        # A registry adds a second name source, so names must be namespaced then.
        self._single = len(self._backends) == 1 and not self._registry.ids()
        self._health_interval = health_interval
        self._health_tasks: list[asyncio.Task] = []
        # Progressive disclosure (SEP §6): over the threshold, advertise a curated
        # prefix plus a proxy__search_tools meta-tool instead of the full list.
        self._disclose = disclose
        self._disclose_threshold = disclose_threshold

    def set_list_directives(self, ttl_ms: int, cache_scope: str) -> "ForwardRouter":
        """Advertise SEP-2549 cache directives on served list results (σ0, §5), so
        a downstream cache can cache yamp's own composed surface."""
        self._list_directives = (ttl_ms, cache_scope)
        return self

    def set_validate_schemas(self, on: bool) -> "ForwardRouter":
        """Validate local-handler calls against their declared schemas (σ1, §5): a
        ``tools/call``'s arguments against the tool's ``inputSchema`` before the
        handler runs, and the result against ``outputSchema`` before it leaves.
        Off by default; a routed backend's calls are never validated (the proxy
        role does not assume a schema it did not author)."""
        self._validate_schemas = on
        return self

    def set_worker_pool(self, cap: int, idle_ms: int = 0) -> "ForwardRouter":
        """Serve server-originated calls from a bounded worker pool (σ2, §5). A
        ``tools/call`` that resolves to a local handler then runs concurrently
        under a per-connection cap (``cap`` simultaneous calls; ``0`` is
        unbounded), is cancellable by ``notifications/cancelled``, and is bounded
        by an idle deadline (``idle_ms``; ``0`` means no deadline). Off by default,
        so the route loop stays serial and existing flows byte-identical."""
        self._pool_enabled = True
        self._pool_cap = cap
        self._pool_idle_ms = idle_ms
        self._pool_sem = asyncio.Semaphore(cap) if cap > 0 else None
        return self

    def set_server_tasks(self, on: bool) -> "ForwardRouter":
        """Originate server-side task handles for task-augmented local calls (σ3,
        §5). A ``tools/call`` that resolves to a local handler and carries the task
        augmentation then returns a ``working`` handle at once, runs in the
        background, and its later ``tasks/get``/``tasks/cancel`` are served from the
        store. Off by default; when off a task-augmented call is answered
        synchronously (the augmentation is ignored)."""
        self._server_tasks = on
        return self

    def set_resource_subscriptions(self, on: bool) -> "ForwardRouter":
        """Originate server-side resource subscriptions (σ4, §5). A
        ``resources/subscribe`` whose URI does not resolve to a backend is then
        registered in a per-connection registry (an empty result is returned), and
        :meth:`publish_resource_update` sends ``notifications/resources/updated``
        only to the subscribed URIs. Off by default; when off such a subscribe is
        rejected as an unknown resource. Proxy-side subscribe/unsubscribe to a
        backend routes regardless of this toggle."""
        self._subscriptions_enabled = on
        return self

    def set_output_limit(self, max_bytes: int) -> "ForwardRouter":
        """Cap the encoded size of a server-originated result (σ5, §5). A local
        handler's result over ``max_bytes`` is rejected with a server-class error
        (`-32603`) instead of emitted, so a runaway handler cannot force an
        unbounded frame. ``0`` disables the cap. Defaults to the frame ceiling
        (`MAX_OUTPUT_BYTES`), which never trips in normal use. The proxy role never
        caps a routed backend response (the decoder that read it already bounded
        it)."""
        self._output_limit = max_bytes
        return self

    def set_drain_timeout(self, drain_ms: int) -> "ForwardRouter":
        """Drain in-flight server-originated work gracefully on shutdown (σ5, §5).
        When the client disconnects, running pooled calls and server tasks are
        given up to ``drain_ms`` to finish (and send their responses) before being
        cancelled. ``0`` (the default) cancels immediately, the σ2/σ3 behavior."""
        self._drain_ms = drain_ms
        return self

    async def publish_resource_update(self, uri: str) -> bool:
        """Server role (σ4): notify the client of a resource change, but only if it
        subscribed to ``uri``. Returns whether a notification was sent, so an
        unsubscribed resource costs nothing (the efficient fan-out)."""
        if not self._subscriptions.contains(uri):
            return False
        await self._send_client(jsonrpc.encode(subscriptions.updated_notification(uri)))
        return True

    async def serve(self) -> None:
        if not await self._handshake():
            return
        if self._resilient and self._health_interval:
            self._health_tasks = [
                asyncio.create_task(self._health_loop(backend))
                for backend in self._backends.values()
            ]
        await self._route_loop()
        await self._drain()
        for task in self._health_tasks:
            task.cancel()
        for backend in self._backends.values():
            await backend.close()

    async def _drain(self) -> None:
        # Drain the worker pool and any running server tasks at shutdown (σ2/σ3/σ5).
        # With a drain window (σ5) the in-flight work is first given up to
        # _drain_ms to finish and send its response; whatever is still running past
        # the window is then cancelled. A window of 0 cancels immediately. Either
        # way the unwinding is awaited so nothing is left dangling.
        pending = list(self._pool_tasks.values()) + list(self._task_handles.values())
        if not pending:
            return
        if self._drain_ms > 0:
            _, pending_set = await asyncio.wait(pending, timeout=self._drain_ms / 1000)
            pending = list(pending_set)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _health_loop(self, backend: Backend) -> None:
        while True:
            await asyncio.sleep(self._health_interval)
            await self._health_check_once(backend)

    async def _health_check_once(self, backend: Backend) -> None:
        # A ping succeeds or fails; request() records the breaker outcome. A
        # backend in half-open recovers here, and a change is announced.
        try:
            await backend.request("ping", {})
        except Exception:
            pass
        await self.emit_if_surface_changed()

    def _available_ids(self) -> frozenset[str]:
        return frozenset(bid for bid, backend in self._backends.items() if backend.available())

    async def emit_if_surface_changed(self) -> None:
        """Send tools/list_changed when the set of available backends changes."""
        current = self._available_ids()
        if current != self._surface:
            # A backend that left the available set has an open breaker; drop its
            # cached lists so a recovered backend is re-fetched (SEP §6.2).
            if self._cache is not None:
                for backend_id in self._surface - current:
                    self._cache.invalidate_backend(backend_id)
            self._surface = current
            await self._send_client(
                jsonrpc.encode(jsonrpc.notification("notifications/tools/list_changed"))
            )

    async def _send_client(self, payload: bytes) -> None:
        async with self._client_lock:
            await self._client.send(payload)

    async def _server_message(self, backend_id: str, message: jsonrpc.Message) -> None:
        # A backend's own list_changed invalidates its cached lists (SEP §6.2)
        # before the notification is relayed onward.
        if self._cache is not None and jsonrpc.method_of(message) in _LIST_CHANGED_METHODS:
            self._cache.invalidate_backend(backend_id)
        # A task event carries the backend's own taskId; re-namespace it to the
        # backend__taskId the client holds before relaying (SEP-2694).
        if jsonrpc.method_of(message) == tasks.TASK_EVENT_METHOD:
            message = tasks.namespace_event(message, backend_id)
        # A backend's resources/updated carries the backend's own uri; re-namespace
        # it to the backend__uri the client subscribed with before relaying (σ4).
        # Single backend passes names through, so its uri is not rewritten (matching
        # subscribe passthrough).
        if not self._single and jsonrpc.method_of(message) == subscriptions.UPDATED_METHOD:
            message = subscriptions.namespace_updated(message, backend_id)
        # A backend-initiated request (both id and method) is correlated so the
        # client's reply can be routed back here. The client-facing id is unique
        # across backends; the backend's own id is restored on the reply.
        if "id" in message and jsonrpc.method_of(message) is not None:
            self._server_request_seq += 1
            client_id = f"srv-{self._server_request_seq}"
            self._server_requests[client_id] = (backend_id, message["id"])
            message = dict(message)
            message["id"] = client_id
        if self._on_server_message is not None:
            await self._on_server_message(backend_id, message)
        else:
            await self._send_client(jsonrpc.encode(message))

    async def _send_to_backend(self, backend: "Backend", message: jsonrpc.Message) -> None:
        # Best-effort relay to a backend for messages the proxy does not
        # correlate a reply for. A backend that has died must not take the whole
        # router down, so a send failure is swallowed.
        try:
            await backend.send_message(message)
        except Exception:
            pass

    async def _forward_client_notification(self, message: jsonrpc.Message) -> None:
        # A cancellation names one in-flight request, so it is delivered to the
        # single backend holding it rather than broadcast (SEP §5.1, corpus
        # SEP-2260/2322).
        if jsonrpc.method_of(message) == "notifications/cancelled":
            await self._route_client_cancellation(message)
            return
        # A progress notification for a pooled call resets that call's idle
        # deadline (σ2) before the notification is also broadcast onward.
        if jsonrpc.method_of(message) == "notifications/progress":
            self._touch_progress(message)
        # A generic client notification has no routing key, so it is broadcast
        # onward instead of dropped (SEP §5.1).
        for backend in self._backends.values():
            await self._send_to_backend(backend, message)

    async def _route_client_cancellation(self, message: jsonrpc.Message) -> None:
        request_id = _params(message).get("requestId")
        # A pooled server-originated call the client abandoned: cancel the running
        # task (σ2). Per MCP the receiver stops and sends no response.
        task = self._pool_tasks.get(request_id)
        if task is not None:
            task.cancel()
            return
        # Otherwise the cancelled requestId is a client-facing id the proxy minted
        # for a backend-initiated request (SEP §5.1). Restore the backend's own id
        # and deliver the cancellation only to that backend. An id the proxy is not
        # holding (for example the client cancelling its own already-completed
        # call, whose id no backend ever saw) is dropped rather than broadcast,
        # since a stray requestId names nothing a backend can act on.
        routed = self._server_requests.pop(request_id, None)
        if routed is None:
            return
        backend_id, backend_original_id = routed
        forwarded = dict(message)
        forwarded["params"] = {**_params(message), "requestId": backend_original_id}
        await self._send_to_backend(self._backends[backend_id], forwarded)

    async def _route_client_reply(self, message: jsonrpc.Message) -> None:
        # A client response to a backend-initiated request: restore the backend's
        # own id and send it back to that backend (SEP §5.1). An id with no known
        # correlation is a stray response and is dropped.
        routed = self._server_requests.pop(message.get("id"), None)
        if routed is None:
            return
        backend_id, backend_original_id = routed
        reply = dict(message)
        reply["id"] = backend_original_id
        await self._send_to_backend(self._backends[backend_id], reply)

    # --- Worker pool for server-originated calls (σ2) ---

    def _is_local_call(self, message: jsonrpc.Message, cap: Capability) -> bool:
        # Whether a call resolves to a local handler, the only calls the worker
        # pool serves (a routed call stays on the serial path). Only tools/call
        # has local handlers. The pre-filter name is used to decide; a filter that
        # renamed a routed call into a local one is not a real case.
        if cap.call_method != "tools/call":
            return False
        resolved = cap.split(_params(message).get(cap.field, ""))
        return resolved is not None and self._registry.handler_for(resolved[0]) is not None

    def _now_ms(self) -> int:
        return int(asyncio.get_running_loop().time() * 1000)

    def _spawn_pooled_call(self, message: jsonrpc.Message, cap: Capability) -> None:
        call_id = message["id"]
        meta = _params(message).get("_meta")
        token = meta.get("progressToken") if isinstance(meta, dict) else None
        if token is not None:
            self._pool_token_to_call[token] = call_id
        task = asyncio.ensure_future(self._run_pooled_call(message, cap, call_id))
        self._pool_tasks[call_id] = task

        def _done(_task: asyncio.Task) -> None:
            self._pool_tasks.pop(call_id, None)
            if token is not None:
                self._pool_token_to_call.pop(token, None)

        task.add_done_callback(_done)

    async def _run_pooled_call(self, message: jsonrpc.Message, cap: Capability, call_id: object) -> None:
        # Bound concurrency by the semaphore (a cap of 0 is unbounded), register
        # the call, run it under its idle deadline, and send its response. A
        # notifications/cancelled cancels this task, which unwinds here and sends
        # nothing (MCP cancellation semantics).
        slot = self._pool_sem if self._pool_sem is not None else contextlib.nullcontext()
        try:
            async with slot:
                self._inflight.register(call_id, pool.deadline(self._now_ms(), self._pool_idle_ms))
                try:
                    routed = self._route_call(message, cap)
                    if self._pool_idle_ms > 0:
                        response = await asyncio.wait_for(routed, self._pool_idle_ms / 1000)
                    else:
                        response = await routed
                except asyncio.TimeoutError:
                    response = jsonrpc.error_response(
                        call_id, errors.error_object(errors.INTERNAL_ERROR, "call exceeded idle deadline")
                    )
                finally:
                    self._inflight.remove(call_id)
                await self._send_client(jsonrpc.encode(response))
        except asyncio.CancelledError:
            self._inflight.remove(call_id)
            raise

    def _touch_progress(self, message: jsonrpc.Message) -> None:
        token = pool.progress_token(message)
        call_id = self._pool_token_to_call.get(token) if token is not None else None
        if call_id is not None:
            self._inflight.touch(call_id, pool.deadline(self._now_ms(), self._pool_idle_ms))

    # --- Server-side task origination (σ3) ---

    def _originate_task(self, message: jsonrpc.Message, cap: Capability) -> jsonrpc.Message:
        # Create the task, start its background execution, and return the working
        # handle at once. The client polls tasks/get for the outcome.
        task_id = self._tasks_store.create()
        task = asyncio.ensure_future(self._run_task(message, cap, task_id))
        self._task_handles[task_id] = task
        task.add_done_callback(lambda _t: self._task_handles.pop(task_id, None))
        return jsonrpc.result(message["id"], self._trace_result(tasks.task_handle(task_id, tasks.STATUS_WORKING)))

    async def _run_task(self, message: jsonrpc.Message, cap: Capability, task_id: str) -> None:
        # Run the call to completion and record its outcome in the store. A
        # cancellation (tasks/cancel) unwinds here; the store is already marked
        # cancelled by the handler, so this only stops the work.
        try:
            response = await self._route_call(message, cap)
        except asyncio.CancelledError:
            self._tasks_store.cancel(task_id)
            raise
        except Exception:
            self._tasks_store.fail(task_id, errors.error_object(INTERNAL_ERROR, "task execution failed"))
            return
        if "result" in response:
            self._tasks_store.complete(task_id, response["result"])
        else:
            self._tasks_store.fail(task_id, response.get("error", errors.error_object(INTERNAL_ERROR)))

    def _serve_local_task(self, message: jsonrpc.Message, method: str, task_id: str) -> jsonrpc.Message:
        # Serve a server-originated task from the store (σ3). tasks/cancel aborts
        # the running execution and marks the task cancelled; every method returns
        # the task's current handle (status plus result or error).
        if method == "tasks/cancel":
            handle = self._task_handles.pop(task_id, None)
            if handle is not None:
                handle.cancel()
            self._tasks_store.cancel(task_id)
        record = self._tasks_store.get(task_id)
        return jsonrpc.result(
            message["id"],
            self._trace_result(tasks.task_handle(task_id, record["status"], record.get("result"), record.get("error"))),
        )

    async def _handshake(self) -> bool:
        raw = await self._client.receive()
        if raw is None:
            return False
        client_init = jsonrpc.decode(raw)
        if jsonrpc.method_of(client_init) != "initialize":
            await self._send_client(
                jsonrpc.encode(
                    jsonrpc.error(
                        client_init.get("id"), INVALID_REQUEST, "expected initialize"
                    )
                )
            )
            raise HandshakeError("first client message was not initialize")

        # Validate the client's token claims before trusting it (SEP-2468,
        # confused deputy). Claims travel in the initialize _meta.
        if self._issuer is not None or self._audience is not None:
            claims = _params(client_init).get("_meta", {}).get(auth.CLAIMS_META_KEY, {})
            if not auth.claims_valid(claims, self._issuer, self._audience):
                await self._send_client(
                    jsonrpc.encode(jsonrpc.error(client_init.get("id"), UNAUTHORIZED, "invalid token claims"))
                )
                raise HandshakeError("client token claims failed iss/aud validation")

        client_caps = _params(client_init).get("capabilities", {})
        backend_caps: list[jsonrpc.Message] = []
        for backend in self._backends.values():
            backend.start(self._server_message)
            try:
                await backend.handshake()
            except Exception:
                # In resilient mode a backend that is down at startup opens its
                # breaker and is left out; it does not fail the whole router.
                if not self._resilient or backend.breaker is None:
                    raise
                backend.breaker.record_failure()
                continue
            backend_caps.append(backend.capabilities)
        # Compose per SEP §2.3 instead of last-writer-wins: sampling/logging if
        # any backend, elicitation if the client, extensions unioned.
        capabilities = capability.compose_capabilities(backend_caps, client_caps)
        # Local handlers contribute tools too, so advertise the tools capability.
        if self._registry.ids():
            capabilities.setdefault("tools", {})
        # Compose server variants across backends (SEP-2053): only variants every
        # variant-supporting backend offers are exposed, since the proxy cannot
        # honestly serve one a backend cannot. The naive extension union replaces
        # the payload with a single backend's copy, so overwrite it here.
        composed_variants = variants.compose_variants(backend_caps)
        if composed_variants:
            capabilities.setdefault("extensions", {})[variants.EXTENSION_ID] = {"availableVariants": composed_variants}
            self._variants = [v["id"] for v in composed_variants]
        else:
            self._variants = []
            extensions = capabilities.get("extensions")
            if isinstance(extensions, dict):
                extensions.pop(variants.EXTENSION_ID, None)
                if not extensions:
                    capabilities.pop("extensions", None)
        self._surface = self._available_ids()

        await self._send_client(
            jsonrpc.encode(
                jsonrpc.result(
                    client_init.get("id"),
                    {
                        "protocolVersion": PROXY_PROTOCOL_VERSION,
                        "capabilities": capabilities,
                        "serverInfo": PROXY_SERVER_INFO,
                    },
                )
            )
        )
        await self._client.receive()  # consume notifications/initialized
        return True

    async def _route_loop(self) -> None:
        while True:
            raw = await self._client.receive()
            if raw is None:
                return
            message = jsonrpc.decode(raw)
            method = jsonrpc.method_of(message)
            if method is None and "id" in message:
                # A response with no method: the client replying to a
                # backend-initiated request. Route it back to the backend.
                await self._route_client_reply(message)
                continue
            if "id" not in message:
                # A client notification: forward it onward (SEP §5.1).
                await self._forward_client_notification(message)
                continue
            cap = _LIST_METHODS.get(method)
            call = _CALL_METHODS.get(method)
            params = _params(message)
            list_filter = params.get("filter")
            # Per-request server variant (SEP-2053): selected via _meta, forwarded
            # to backends, and rejected here if the composed set does not offer it.
            variant = variants.selected_variant(params)
            cursor = params.get("cursor")
            if method in tasks.TASKS_METHODS:
                response = await self._route_task(message, method)
            elif subscriptions.is_subscribe_method(method):
                response = await self._route_subscription(message, method)
            elif (variant_error := self._variant_error(message["id"], variant)) is not None:
                response = variant_error
            elif method == _DISCOVER_METHOD:
                response = await self._aggregate(message["id"], _TOOLS_CAPABILITY, list_filter, cursor, variant)
            elif cap is not None:
                response = await self._aggregate(message["id"], cap, list_filter, cursor, variant)
            elif call is not None:
                if self._server_tasks and self._is_local_call(message, call) and tasks.is_task_augmented(params):
                    # Task-augmented server-originated call (σ3): return a working
                    # handle now and run the call in the background.
                    response = self._originate_task(message, call)
                elif self._pool_enabled and self._is_local_call(message, call):
                    # Server-originated: run it in the worker pool and keep reading,
                    # so a later notifications/cancelled can reach it (σ2). The task
                    # sends its own response.
                    self._spawn_pooled_call(message, call)
                    continue
                else:
                    response = await self._route_call(message, call)
            else:
                response = jsonrpc.error(
                    message["id"], METHOD_NOT_FOUND, f"method not routable: {method}"
                )
            await self._send_client(jsonrpc.encode(response))
            if self._resilient:
                await self.emit_if_surface_changed()

    def _variant_error(self, id: object, variant: str | None) -> jsonrpc.Message | None:
        # Reject a per-request variant the composed set cannot serve (SEP-2053).
        # No selection is always fine (the default variant applies).
        if variant is None:
            return None
        if not self._variants:
            return jsonrpc.error(id, INVALID_PARAMS, "Server variants not supported")
        if variant not in self._variants:
            return jsonrpc.error(id, INVALID_PARAMS, f"unknown variant: {variant}", {"availableVariants": list(self._variants)})
        return None

    def _effective_variant(self, variant: str | None) -> str | None:
        # The variant that actually applies: the selection, or the default (first
        # composed) when the request omits one (SEP-2053 default rule).
        if variant is not None:
            return variant
        return self._variants[0] if self._variants else None

    def _label(self, cap: Capability, backend_id: str, value: str) -> str:
        # Single-backend: pass names through unmodified (SEP §5.3).
        return value if self._single else cap.prefix(backend_id, value)

    def _trace_request(self, params: jsonrpc.Message) -> jsonrpc.Message:
        # Add this proxy's hop and a trace context to a request forwarded to a
        # backend (SEP §7.1, §7.2).
        if not self._trace:
            return params
        meta = observability.append_hop(dict(params.get("_meta", {})), mode="forward")
        meta = observability.ensure_trace_context(meta)
        out = dict(params)
        out["_meta"] = meta
        return out

    def _trace_result(self, result: jsonrpc.Message) -> jsonrpc.Message:
        # Add this proxy's hop to a result forwarded to the client (SEP §7.1).
        if not self._trace:
            return result
        out = dict(result)
        out["_meta"] = observability.append_hop(dict(result.get("_meta", {})), mode="forward")
        return out

    def _record_audit(self, record: jsonrpc.Message) -> None:
        # Best-effort accountability (SEP-2787/2828): appending to the in-memory,
        # append-only log is a trivial synchronous op, so it never blocks traffic.
        if self._audit is not None:
            self._audit.append(record)

    async def _collect(
        self,
        cap: Capability,
        list_filter: jsonrpc.Message | None = None,
        cursor: object = None,
        variant: str | None = None,
    ) -> tuple[list[jsonrpc.Message], list[str], dict[str, str]]:
        # Query each backend the breaker permits; a fresh cache hit skips the
        # request entirely (SEP §6). One in-flight request per uncached backend
        # since each is a distinct transport.
        list_filter = list_filter or {}
        keywords = list_filter.get("keywords", [])
        patterns = list_filter.get("namePatterns", [])
        # A proxy composite cursor (SEP-2053) restricts the continuation to the
        # backends that still have pages, each with its own backend cursor.
        resolved_cursor = variants.resolve_cursor(cursor)
        restrict = resolved_cursor[1] if resolved_cursor is not None else None
        effective_variant = self._effective_variant(variant)
        # Keyword pre-select (SEP-2614): skip backends that cannot match, cutting
        # fan-out. A filtered list is forwarded to the backend (SEP-2564).
        available = [
            b
            for b in self._backends.values()
            if (not self._resilient or b.available())
            and routing.backend_selected(b.keywords, keywords)
            and (restrict is None or b.id in restrict)
        ]
        unavailable = [bid for bid, b in self._backends.items() if self._resilient and not b.available()]

        def _params_for(backend: Backend) -> jsonrpc.Message:
            # The shared filter, the active variant in _meta (SEP-2053), and a
            # continuation cursor: each backend's own cursor on a composite
            # continuation, or a raw cursor passed straight through in single mode.
            params: jsonrpc.Message = {}
            if list_filter:
                params["filter"] = list_filter
            if effective_variant is not None:
                params["_meta"] = {variants.SERVER_VARIANT_META_KEY: effective_variant}
            if restrict is not None:
                params["cursor"] = restrict[backend.id]
            elif cursor is not None and self._single:
                params["cursor"] = cursor
            return params

        # A filter, a cursor, or an active variant makes the fetch specific, so
        # only reuse the cache for the plain unfiltered default surface.
        use_cache = (
            self._cache is not None and not list_filter and cursor is None and effective_variant is None
        )
        results: dict[str, jsonrpc.Message] = {}
        to_fetch = []
        for backend in available:
            cached = self._cache.get(backend.id, cap.list_method, self._principal) if use_cache else None
            if cached is not None:
                results[backend.id] = cached
            else:
                to_fetch.append(backend)
        responses = await asyncio.gather(
            *(b.request(cap.list_method, _params_for(b)) for b in to_fetch), return_exceptions=True
        )
        for backend, response in zip(to_fetch, responses):
            if isinstance(response, Exception):
                if not self._resilient:
                    raise response
                unavailable.append(backend.id)
                continue
            result = response.get("result", {})
            if use_cache:
                self._cache.put(backend.id, cap.list_method, self._principal, result)
            results[backend.id] = result
        # A backend that returned a nextCursor still has pages; carry each one so
        # the aggregator can mint a single variant-bound composite cursor.
        next_cursors = {
            backend.id: results[backend.id]["nextCursor"]
            for backend in available
            if isinstance(results.get(backend.id, {}).get("nextCursor"), str)
        }
        aggregated: list[jsonrpc.Message] = []
        for backend in available:
            result = results.get(backend.id)
            if result is None:
                continue
            for item in result.get(cap.collection, []):
                # Collision labeling (SEP §3.4). prefix/priority/manual all label
                # with the backend prefix; manual is remapped to its exposed name
                # in _aggregate, where the full name set is known and a collision
                # can be rejected. passthrough keeps the original name and records
                # a reverse map so a later tools/call still resolves.
                passthrough = (
                    cap.collection == "tools"
                    and self._namespacing.strategy == collision.PASSTHROUGH
                    and not self._single
                )
                if passthrough:
                    labeled = item[cap.field]
                    self._reverse.setdefault(labeled, (backend.id, item[cap.field]))
                else:
                    labeled = self._label(cap, backend.id, item[cap.field])
                # Server-side name filtering on the composed surface (SEP-2564).
                if not routing.name_matches(labeled, patterns):
                    continue
                entry = dict(item)
                entry[cap.field] = labeled
                aggregated.append(entry)
        return aggregated, unavailable, next_cursors

    async def _aggregate(
        self,
        id: object,
        cap: Capability,
        list_filter: jsonrpc.Message | None = None,
        cursor: object = None,
        variant: str | None = None,
    ) -> jsonrpc.Message:
        # Variant-bound cursor validation (SEP-2053 rule 3): a proxy composite
        # cursor may only be used under the variant it was minted with; a cursor
        # the proxy did not mint cannot be routed by an aggregator.
        resolved_cursor = variants.resolve_cursor(cursor)
        effective_variant = self._effective_variant(variant)
        if resolved_cursor is not None:
            cursor_variant = resolved_cursor[0]
            if cursor_variant != effective_variant:
                return jsonrpc.error(
                    id,
                    INVALID_PARAMS,
                    "Cursor invalid for requested variant",
                    variants.mismatch_data(cursor_variant, effective_variant),
                )
        elif cursor is not None and not self._single:
            return jsonrpc.error(id, INVALID_PARAMS, "unknown cursor")
        aggregated, unavailable, next_cursors = await self._collect(cap, list_filter, cursor, variant)
        if cap.collection == "tools":
            # Merge local handler tools into the routed backend surface (dispatch
            # seam): the client sees one namespaced tools/list. Local tools honor
            # the same name filter.
            patterns = (list_filter or {}).get("namePatterns", [])
            local = [t for t in self._registry.list_tools() if routing.name_matches(t[cap.field], patterns)]
            aggregated = aggregated + local
        if cap.collection == "tools" and self._namespacing.strategy == collision.MANUAL:
            # Apply explicit renames to the composed (prefixed) surface. The full
            # name set is known here, so an unresolved collision (two names mapping
            # to one exposed name) is rejected rather than served as a silent
            # duplicate (SEP §3.4).
            try:
                mapping = collision.resolve_manual(
                    [t[cap.field] for t in aggregated], self._namespacing.overrides
                )
            except collision.UnresolvedCollision as exc:
                return jsonrpc.error(id, INTERNAL_ERROR, f"manual collision: {exc}")
            for item in aggregated:
                item[cap.field] = mapping[item[cap.field]]
        if cap.collection == "tools" and self._namespacing.strategy == collision.PRIORITY:
            # Names stay prefixed; drop the lower-priority copy of a duplicated
            # original name (SEP §3.4). Reverse resolution is unaffected.
            aggregated = collision.apply_priority(
                aggregated, cap.split, self._namespacing.priority, field=cap.field
            )
        if self._disclose and cap.collection == "tools":
            aggregated, _ = capability.disclose(aggregated, self._disclose_threshold)
        body: jsonrpc.Message = {cap.collection: aggregated}
        # Mint one opaque composite cursor binding the active variant and every
        # paginating backend's own cursor (SEP-2053 rule 2), so the continuation
        # routes back to exactly those backends under the same variant.
        if next_cursors:
            body["nextCursor"] = variants.bind_cursor(effective_variant, next_cursors)
        if unavailable:
            body["_meta"] = partial_meta(unavailable, "backend_unavailable")
        # Attach the server's cache directives to the served list (σ0, §5).
        if self._list_directives is not None:
            body = server.attach_directives(body, *self._list_directives)
        return jsonrpc.result(id, self._trace_result(body))

    async def _route_call(self, message: jsonrpc.Message, cap: Capability) -> jsonrpc.Message:
        # Extension filter chain (ε0): scan the request before routing. A block
        # returns a clean -32001 (plus a best-effort audit outcome); a forward
        # may carry a mutated request (substituted arguments, annotated _meta)
        # that flows onward. None means no filters, so the seam costs nothing.
        if self._filter_chain is not None:
            outcome = self._filter_chain.run(filters.REQUEST, message)
            if outcome["action"] == "block":
                name = _params(message).get(cap.field, "")
                self._record_audit(signing.outcome_record(cap.call_method, name, False))
                return outcome["response"]
            message = outcome["message"]
        params = _params(message)
        value = params.get(cap.field, "")
        if self._disclose and cap.call_method == "tools/call" and value == capability.PROXY_SEARCH_TOOL:
            # The search meta-tool is served by the proxy, not routed.
            tools, _, _ = await self._collect(_CAPABILITIES[0])
            matches = capability.search_tools(params.get("arguments", {}).get("query", ""), tools)
            result = {"content": [{"type": "text", "text": json.dumps([t["name"] for t in matches])}]}
            return jsonrpc.result(message["id"], self._trace_result(result))
        # Dispatch seam: a tools/call whose prefix names a local handler is served
        # here (server behavior) instead of routed (draft §5.3/§5.7).
        if cap.call_method == "tools/call":
            resolved = cap.split(value)
            if resolved is not None:
                handler = self._registry.handler_for(resolved[0])
                if handler is not None:
                    arguments = params.get("arguments", {})
                    # σ1: validate against the tool's declared schemas (server
                    # role, off by default). The tool definition carries the
                    # schemas under its original (un-prefixed) name.
                    tool = None
                    if self._validate_schemas:
                        tool = next((t for t in handler.list_tools() if t.get("name") == resolved[1]), None)
                    if tool is not None:
                        err = schema.validate_call_args(tool.get("inputSchema"), arguments)
                        if err is not None:
                            return jsonrpc.error_response(message["id"], err)
                    result = await handler.call_tool(resolved[1], arguments)
                    if tool is not None:
                        err = schema.validate_call_result(tool.get("outputSchema"), result)
                        if err is not None:
                            return jsonrpc.error_response(message["id"], err)
                    # σ5: bound the server's own output. An oversize local result is
                    # the server's fault, so it is a server-class error.
                    if server.exceeds_output_cap(result, self._output_limit):
                        return jsonrpc.error_response(message["id"], errors.error_object(INTERNAL_ERROR, "server output exceeds size limit"))
                    return jsonrpc.result(message["id"], self._trace_result(result))
        manual = cap.call_method == "tools/call" and self._namespacing.strategy == collision.MANUAL
        passthrough = cap.call_method == "tools/call" and self._namespacing.strategy == collision.PASSTHROUGH
        if self._single:
            backend_id = next(iter(self._backends))
            original = value
        elif manual:
            # Invert the explicit renames (exposed -> namespaced), then split. A
            # name that was not renamed is already its namespaced form.
            inverse = {exposed: namespaced for namespaced, exposed in self._namespacing.overrides.items()}
            resolved = namespace.split(inverse.get(value, value))
            if resolved is None or resolved[0] not in self._backends:
                return jsonrpc.error(message["id"], INVALID_PARAMS, f"unknown {cap.field}: {value}")
            backend_id, original = resolved
        elif passthrough:
            # Resolve the original name through the reverse map, warming it with a
            # list fan-out if this is the first call before any list.
            if value not in self._reverse:
                await self._collect(_TOOLS_CAPABILITY)
            resolved = self._reverse.get(value)
            if resolved is None or resolved[0] not in self._backends:
                return jsonrpc.error(message["id"], INVALID_PARAMS, f"unknown {cap.field}: {value}")
            backend_id, original = resolved
        else:
            resolved = cap.split(value)
            if resolved is None or resolved[0] not in self._backends:
                return jsonrpc.error(message["id"], INVALID_PARAMS, f"unknown {cap.field}: {value}")
            backend_id, original = resolved
        backend = self._backends[backend_id]
        if self._resilient and not backend.available():
            return jsonrpc.error(
                message["id"], SERVER_NOT_AVAILABLE, f"backend unavailable: {backend_id}"
            )
        forwarded = dict(params)
        forwarded[cap.field] = original
        forwarded = self._trace_request(forwarded)
        # Accountability (SEP-2787/2828): a pre-call attestation, then a paired
        # outcome once the call resolves. Best-effort; never affects the reply.
        principal = self._principal or "anonymous"
        self._record_audit(signing.attestation_record(principal, cap.call_method, value))
        try:
            response = await backend.request(cap.call_method, forwarded)
        except Exception:
            if not self._resilient:
                raise
            self._record_audit(signing.outcome_record(cap.call_method, value, False))
            return jsonrpc.error(
                message["id"], SERVER_NOT_AVAILABLE, f"backend call failed: {backend_id}"
            )
        self._record_audit(signing.outcome_record(cap.call_method, value, "result" in response))
        if "result" in response:
            result = response["result"]
            # A task-augmented tools/call returns a task handle; namespace its id
            # so the client's later tasks/* requests route back here (SEP-2663).
            if cap.call_method == "tools/call" and tasks.is_task_result(result):
                result = tasks.namespace_task_id(result, backend_id)
            return jsonrpc.result(message["id"], self._trace_result(result))
        return {
            "jsonrpc": "2.0",
            "id": message["id"],
            "error": response.get("error", {"code": INTERNAL_ERROR, "message": "backend error"}),
        }

    async def _route_task(self, message: jsonrpc.Message, method: str) -> jsonrpc.Message:
        # Route a tasks/* request to the backend that holds the task's state,
        # reverse-resolving the namespaced taskId (SEP-2663). Reads and writes
        # both route the same way; the read/write split matters only for caching.
        params = _params(message)
        task_id = params.get("taskId", "")
        # A server-originated task (σ3) is served from the local store; its id
        # carries no `__`, so it never collides with a routed backend__taskId.
        if self._server_tasks and self._tasks_store.contains(task_id):
            return self._serve_local_task(message, method, task_id)
        resolved = tasks.resolve_task(task_id)
        if resolved is None or resolved[0] not in self._backends:
            return jsonrpc.error(message["id"], INVALID_PARAMS, f"unknown task: {task_id}")
        backend_id, original = resolved
        backend = self._backends[backend_id]
        if self._resilient and not backend.available():
            return jsonrpc.error(message["id"], SERVER_NOT_AVAILABLE, f"backend unavailable: {backend_id}")
        forwarded = dict(params)
        forwarded["taskId"] = original  # backend sees its own id
        forwarded = self._trace_request(forwarded)
        try:
            response = await backend.request(method, forwarded)
        except Exception:
            if not self._resilient:
                raise
            return jsonrpc.error(message["id"], SERVER_NOT_AVAILABLE, f"backend call failed: {backend_id}")
        if "result" in response:
            # Re-namespace the taskId in the reply so the client sees one id.
            return jsonrpc.result(message["id"], self._trace_result(tasks.namespace_task_id(response["result"], backend_id)))
        return {
            "jsonrpc": "2.0",
            "id": message["id"],
            "error": response.get("error", {"code": INTERNAL_ERROR, "message": "backend error"}),
        }

    async def _route_subscription(self, message: jsonrpc.Message, method: str) -> jsonrpc.Message:
        # Route resources/subscribe|unsubscribe (σ4). The URI namespaces exactly
        # like resources/read, so it reverse-resolves to the owning backend. A URI
        # that resolves to no backend is a local resource: with server
        # subscriptions on it is registered here (and served by
        # publish_resource_update), otherwise rejected.
        cap = _RESOURCES_CAPABILITY
        params = _params(message)
        uri = params.get(cap.field, "")
        if self._single:
            backend_id = next(iter(self._backends))
            original = uri
        else:
            resolved = cap.split(uri)
            if resolved is not None and resolved[0] in self._backends:
                backend_id, original = resolved
            elif self._subscriptions_enabled:
                if method == subscriptions.SUBSCRIBE_METHOD:
                    self._subscriptions.subscribe(uri)
                else:
                    self._subscriptions.unsubscribe(uri)
                return jsonrpc.result(message["id"], self._trace_result({}))
            else:
                return jsonrpc.error(message["id"], INVALID_PARAMS, f"unknown resource: {uri}")
        backend = self._backends[backend_id]
        if self._resilient and not backend.available():
            return jsonrpc.error(message["id"], SERVER_NOT_AVAILABLE, f"backend unavailable: {backend_id}")
        forwarded = dict(params)
        forwarded[cap.field] = original  # backend sees its own uri
        forwarded = self._trace_request(forwarded)
        try:
            response = await backend.request(method, forwarded)
        except Exception:
            if not self._resilient:
                raise
            return jsonrpc.error(message["id"], SERVER_NOT_AVAILABLE, f"backend call failed: {backend_id}")
        if "result" in response:
            return jsonrpc.result(message["id"], self._trace_result(response["result"]))
        return {
            "jsonrpc": "2.0",
            "id": message["id"],
            "error": response.get("error", {"code": INTERNAL_ERROR, "message": "backend error"}),
        }
