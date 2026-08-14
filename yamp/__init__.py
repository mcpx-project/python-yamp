"""Yet Another MCP Proxy, Python arm.

δ0: Layer 1 transport substrate and a byte-faithful relay.
δ1: Forward mode, stateful, single backend (dual handshake).
δ2: Forward routing + namespace, multi-backend.
δ3: Forward stateless mode (both protocol modes covered).
δ4: Transparent mode Level 1 (transport-aware).
δ5: Transparent mode Level 2 (protocol-aware). Action complete.
δ6: Layer 4 resilience (circuit breaker, partial failure, no-retry).
δ7-δ9: policy (Layer 5), capability (Layer 7), observability.
"""

from . import auth, callout, collision, content, errors, filters, icap, media, routing, server, signing, tasks
from .cache import ListCache
from .capability import compose, compose_capabilities, disclose, search_tools
from .handler import BackendsHandler, Handler, Registry, build_registry
from .forward import ForwardProxy
from .observability import append_hop, ensure_trace_context
from .policy import PolicyLayer
from .relay import Relay
from .resilience import CircuitBreaker, ManagedBackend, ResilientRouter
from .router import Backend, ForwardRouter
from .stateless import StatelessBackend, StatelessForwarder
from .transparent import HeaderPolicy, TransparentL1
from .transparent_l2 import TransparentL2Stateful, TransparentL2Stateless
from .transport.base import Transport, WriteEnd
from .transport.framed import FramedTransport
from .transport.line import LineTransport
from .transport.memory import MemoryPipe
from .transport.sse import SseTransport
from .version import (
    SUPPORTED_PROTOCOL_VERSIONS,
    UNSUPPORTED_PROTOCOL_VERSION,
    negotiate,
)

__all__ = [
    "Backend",
    "ForwardRouter",
    "ForwardProxy",
    "StatelessBackend",
    "StatelessForwarder",
    "HeaderPolicy",
    "TransparentL1",
    "TransparentL2Stateless",
    "TransparentL2Stateful",
    "CircuitBreaker",
    "ManagedBackend",
    "ResilientRouter",
    "PolicyLayer",
    "ListCache",
    "Handler",
    "Registry",
    "BackendsHandler",
    "build_registry",
    "auth",
    "callout",
    "content",
    "errors",
    "filters",
    "icap",
    "media",
    "routing",
    "server",
    "signing",
    "tasks",
    "collision",
    "compose",
    "compose_capabilities",
    "disclose",
    "search_tools",
    "append_hop",
    "ensure_trace_context",
    "Relay",
    "Transport",
    "WriteEnd",
    "FramedTransport",
    "LineTransport",
    "SseTransport",
    "MemoryPipe",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "UNSUPPORTED_PROTOCOL_VERSION",
    "negotiate",
]
