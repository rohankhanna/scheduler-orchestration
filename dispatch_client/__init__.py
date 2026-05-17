"""Official Python client for Dispatch (scheduler-orchestration).

Consumers must not implement local API shims/adapters. If the API contract is
missing or inconvenient, fix it here (Dispatch) and release a new version.
"""

from dispatch_client.client import DispatchClient, DispatchClientConfig
from dispatch_client.errors import DispatchAPIError

__all__ = ["DispatchClient", "DispatchClientConfig", "DispatchAPIError"]
