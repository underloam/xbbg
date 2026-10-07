"""Static references to Pydantic registrations consumed outside this package.

Vulture parses this file without importing it or requiring the package installed.
Validator and serializer registrations are identified by their exact decorators
in pyproject.toml; all other source and test code remains subject to analysis.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

from xbbg_langgraph._runtime import ToolInput
from xbbg_langgraph.options import BloombergToolsOptions

# Pydantic reads these class attributes while constructing each model.
_ = (ToolInput.model_config, BloombergToolsOptions.model_config)
# unittest.mock reads configured recipe return values when the coroutine is awaited.
_ = AsyncMock.return_value
