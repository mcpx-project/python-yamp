"""Tasks-extension routing (corpus SEP-2663, with SEP-2694 and SEP-2848).

A task-augmented ``tools/call`` returns a task handle (``resultType: "task"``)
with a server-generated ``taskId`` instead of an immediate result; the client
then polls ``tasks/get`` / ``tasks/update`` / ``tasks/cancel`` by that id. An
intermediary MUST route each same-task request to the replica that holds the
task's state (SEP-2663: ``Mcp-Name`` is set to the ``taskId``).

yamp routes tasks the same way it routes tools: the taskId is namespaced as
``backend__taskId`` when the task is created, so a later ``tasks/*`` request
reverse-resolves to the originating backend by splitting the id. Task ids are
server-generated and carry no ``__`` delimiter, so this is stateless and needs
no correlation map. Reads (``tasks/get``, ``tasks/stream``) are separable from
writes for cacheability (SEP-2663), tracked here for a future cache tier.

SEP-2694 (resumable task event streams) adds ``tasks/stream``, which starts or
resumes a task's event stream and delivers ``notifications/tasks/event`` on the
same connection. It routes like any other ``tasks/*`` request (reverse-resolve
the taskId, forward the backend's own id, preserve the ``after`` resume cursor);
the events the backend then emits are re-namespaced so the client sees the same
``backend__taskId`` it holds. SEP-2848 (asynchronous approval for tool calls)
needs no new routing: an approval-gated call returns an ordinary ``working`` task
handle, which is already namespaced and routed, and the optional
``net.openid.authzen/tool-approval`` client extension composes through the
capability extensions union.
"""

from __future__ import annotations

from . import namespace

# tasks/stream (SEP-2694) routes like the other task methods and, like tasks/get,
# is a read (it observes events, it does not mutate task state).
TASKS_METHODS = frozenset({"tasks/get", "tasks/update", "tasks/cancel", "tasks/stream"})
TASK_READ_METHODS = frozenset({"tasks/get", "tasks/stream"})  # cacheable reads vs writes
TASK_EVENT_METHOD = "notifications/tasks/event"  # backend -> client event notification (SEP-2694)
RESULT_TYPE_TASK = "task"

# Server-side origination (σ3): the request-side augmentation marker
# and the task lifecycle statuses. A client opts a tools/call into task execution
# by carrying this key in the request _meta; yamp (as the server) then returns a
# working handle instead of blocking, runs the call in the background, and serves
# the later tasks/get and tasks/cancel from its own store.
TASK_META_KEY = "io.modelcontextprotocol/task"
STATUS_WORKING = "working"
STATUS_COMPLETED = "completed"
STATUS_CANCELLED = "cancelled"
STATUS_FAILED = "failed"


def is_task_result(result: object) -> bool:
    """Whether a ``tools/call`` result is a task handle (SEP-2663)."""
    return isinstance(result, dict) and result.get("resultType") == RESULT_TYPE_TASK


def is_task_augmented(params: dict) -> bool:
    """Whether a request opts into task execution (its ``_meta`` carries the task
    augmentation key). A server that supports tasks then originates a handle
    instead of blocking; one that does not may ignore this and answer directly."""
    meta = params.get("_meta")
    return isinstance(meta, dict) and TASK_META_KEY in meta


def new_task_id(seq: int) -> str:
    """A server-generated task id. It carries no ``__`` delimiter, so it never
    collides with a routed ``backend__taskId`` and reverse resolution treats it as
    local."""
    return f"task-{seq}"


def task_handle(task_id: str, status: str, result: object = None, error: object = None) -> dict:
    """A task handle (``resultType: "task"``): the working handle returned at
    creation, and the status object a later ``tasks/get`` returns. A completed task
    carries its ``result``; a failed one its ``error``."""
    handle = {"resultType": RESULT_TYPE_TASK, "taskId": task_id, "status": status}
    if result is not None:
        handle["result"] = result
    if error is not None:
        handle["error"] = error
    return handle


def namespace_task_id(result: dict, backend_id: str) -> dict:
    """Namespace ``taskId`` (and an embedded ``task.taskId``) under the backend
    so the client's later ``tasks/*`` requests reverse-resolve here."""
    out = dict(result)
    if isinstance(out.get("taskId"), str):
        out["taskId"] = namespace.prefix(backend_id, out["taskId"])
    task = out.get("task")
    if isinstance(task, dict) and isinstance(task.get("taskId"), str):
        task = dict(task)
        task["taskId"] = namespace.prefix(backend_id, task["taskId"])
        out["task"] = task
    return out


def resolve_task(task_id: str) -> tuple[str, str] | None:
    """Reverse-resolve a namespaced task id to ``(backend_id, original_id)``."""
    return namespace.split(task_id)


def namespace_event(message: dict, backend_id: str) -> dict:
    """Re-namespace the ``taskId`` in a backend's ``notifications/tasks/event`` so
    the client sees the same ``backend__taskId`` it holds (SEP-2694). A message
    without a string ``params.taskId`` is returned unchanged."""
    params = message.get("params")
    if not isinstance(params, dict) or not isinstance(params.get("taskId"), str):
        return message
    out = dict(message)
    out_params = dict(params)
    out_params["taskId"] = namespace.prefix(backend_id, params["taskId"])
    out["params"] = out_params
    return out


class ServerTasks:
    """The server's own task store (σ3): the state a server-originated task holds
    between its working handle and its terminal outcome. Terminal states are
    final, so ``complete``/``fail``/``cancel`` transition only from ``working``;
    that makes cancellation and completion racing on the same task deterministic
    (whichever reaches ``working`` first wins). The background execution and its
    cancellation live in the router; this is the bookkeeping they agree on."""

    def __init__(self) -> None:
        self._tasks: dict = {}
        self._seq = 0

    def create(self) -> str:
        """Register a new working task and return its server-generated id."""
        self._seq += 1
        task_id = new_task_id(self._seq)
        self._tasks[task_id] = {"status": STATUS_WORKING}
        return task_id

    def contains(self, task_id: str) -> bool:
        return task_id in self._tasks

    def get(self, task_id: str) -> dict | None:
        return self._tasks.get(task_id)

    def _is_working(self, task_id: str) -> bool:
        record = self._tasks.get(task_id)
        return record is not None and record["status"] == STATUS_WORKING

    def complete(self, task_id: str, result: object) -> None:
        if self._is_working(task_id):
            self._tasks[task_id] = {"status": STATUS_COMPLETED, "result": result}

    def fail(self, task_id: str, error: object) -> None:
        if self._is_working(task_id):
            self._tasks[task_id] = {"status": STATUS_FAILED, "error": error}

    def cancel(self, task_id: str) -> bool:
        """Cancel a working task. Returns whether it transitioned (a task already
        completed, failed, or cancelled is left as it was)."""
        if self._is_working(task_id):
            self._tasks[task_id] = {"status": STATUS_CANCELLED}
            return True
        return False
