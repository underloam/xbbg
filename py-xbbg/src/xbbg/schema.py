"""Bloomberg schema introspection and stub generation.

This module provides access to cached Bloomberg service schemas
and can generate Python type stubs for IDE autocomplete support.

Example:
    >>> import xbbg
    >>> from xbbg.schema import get_schema, list_operations
    >>>
    >>> # Get schema for a service
    >>> schema = await get_schema("//blp/refdata")
    >>>
    >>> # List available operations
    >>> ops = await list_operations("//blp/refdata")
    >>> print(ops)  # ['ReferenceDataRequest', 'HistoricalDataRequest', ...]
    >>>
    >>> # Get enum values for an element
    >>> values = await get_enum_values("//blp/refdata", "ReferenceDataRequest", "periodicitySelection")
"""

from __future__ import annotations

from dataclasses import dataclass
import functools
import json
from pathlib import Path
from typing import Any


@dataclass
class ElementInfo:
    """Schema element information."""

    name: str
    description: str
    data_type: str
    type_name: str
    is_array: bool
    is_optional: bool
    enum_values: list[str] | None
    children: list[ElementInfo]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ElementInfo:
        """Create from dictionary (parsed JSON)."""
        return cls(
            name=d.get("name", ""),
            description=d.get("description", ""),
            data_type=d.get("data_type", ""),
            type_name=d.get("type_name", ""),
            is_array=d.get("is_array", False),
            is_optional=d.get("is_optional", False),
            enum_values=d.get("enum_values"),
            children=[cls.from_dict(c) for c in d.get("children", [])],
        )


@dataclass
class OperationSchema:
    """Schema for a service operation."""

    name: str
    description: str
    request: ElementInfo
    responses: list[ElementInfo]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> OperationSchema:
        """Create from dictionary (parsed JSON)."""
        return cls(
            name=d.get("name", ""),
            description=d.get("description", ""),
            request=ElementInfo.from_dict(d.get("request", {})),
            responses=[ElementInfo.from_dict(r) for r in d.get("responses", [])],
        )


@dataclass
class ServiceSchema:
    """Schema for a Bloomberg service."""

    service: str
    description: str
    operations: list[OperationSchema]
    cached_at: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ServiceSchema:
        """Create from dictionary (parsed JSON)."""
        return cls(
            service=d.get("service", ""),
            description=d.get("description", ""),
            operations=[OperationSchema.from_dict(o) for o in d.get("operations", [])],
            cached_at=d.get("cached_at", ""),
        )

    @classmethod
    def from_json(cls, json_str: str) -> ServiceSchema:
        """Create from JSON string; Rust owns service-schema invalidation."""
        return _parse_service_schema_json(json_str)

    def get_operation(self, name: str) -> OperationSchema | None:
        """Get an operation by name."""
        for op in self.operations:
            if op.name == name:
                return op
        return None


@functools.lru_cache(maxsize=128)
def _parse_service_schema_json(json_str: str) -> ServiceSchema:
    """Parse a Rust-owned schema JSON snapshot into dataclass objects."""
    return ServiceSchema.from_dict(json.loads(json_str))


# Async API functions
async def aget_schema(service: str) -> ServiceSchema:
    """Get schema for a service (async).

    Loads from cache if available, otherwise introspects the service.

    Args:
        service: Service URI (e.g., "//blp/refdata")

    Returns:
        ServiceSchema object with operations and element definitions.
    """
    from .blp import _get_engine

    engine = _get_engine()
    json_str = await engine.get_schema(service)
    return ServiceSchema.from_json(json_str)


async def aget_operation(service: str, operation: str) -> OperationSchema:
    """Get schema for a specific operation (async).

    Args:
        service: Service URI (e.g., "//blp/refdata")
        operation: Operation name (e.g., "ReferenceDataRequest")

    Returns:
        OperationSchema object with request/response definitions.
    """
    from .blp import _get_engine

    engine = _get_engine()
    json_str = await engine.get_operation(service, operation)
    return OperationSchema.from_dict(json.loads(json_str))


async def alist_operations(service: str) -> list[str]:
    """List all operations for a service (async).

    Args:
        service: Service URI (e.g., "//blp/refdata")

    Returns:
        List of operation names.
    """
    from .blp import _get_engine

    engine = _get_engine()
    return await engine.list_operations(service)


async def aget_enum_values(service: str, operation: str, element: str) -> list[str] | None:
    """Get valid enum values for an element (async).

    Args:
        service: Service URI
        operation: Operation name
        element: Element name

    Returns:
        List of valid enum values, or None if not an enum.
    """
    from .blp import _get_engine

    engine = _get_engine()
    return await engine.get_enum_values(service, operation, element)


async def alist_valid_elements(service: str, operation: str) -> list[str] | None:
    """List all valid element names for an operation (async).

    Args:
        service: Service URI
        operation: Operation name

    Returns:
        List of valid element names.
    """
    from .blp import _get_engine

    engine = _get_engine()
    return await engine.list_valid_elements(service, operation)


# Sync API wrappers
def get_schema(service: str) -> ServiceSchema:
    """Get schema for a service (sync wrapper)."""
    from .blp import _run_sync

    return _run_sync("get_schema", aget_schema, (service,), {})


def get_operation(service: str, operation: str) -> OperationSchema:
    """Get schema for a specific operation (sync wrapper)."""
    from .blp import _run_sync

    return _run_sync("get_operation", aget_operation, (service, operation), {})


def list_operations(service: str) -> list[str]:
    """List all operations for a service (sync wrapper)."""
    from .blp import _run_sync

    return _run_sync("list_operations", alist_operations, (service,), {})


def get_enum_values(service: str, operation: str, element: str) -> list[str] | None:
    """Get valid enum values for an element (sync wrapper)."""
    from .blp import _run_sync

    return _run_sync("get_enum_values", aget_enum_values, (service, operation, element), {})


def list_valid_elements(service: str, operation: str) -> list[str] | None:
    """List all valid element names for an operation (sync wrapper)."""
    from .blp import _run_sync

    return _run_sync("list_valid_elements", alist_valid_elements, (service, operation), {})


# Cache management
def list_cached_schemas() -> list[str]:
    """List all cached service URIs."""
    from .blp import _get_engine

    engine = _get_engine()
    return engine.list_cached_schemas()


def invalidate_schema(service: str) -> None:
    """Invalidate a cached schema."""
    from . import blp

    engine = blp._get_engine()
    engine.invalidate_schema(service)
    _parse_service_schema_json.cache_clear()
    blp._clear_field_type_resolution_cache()


def clear_schema_cache() -> None:
    """Clear all cached schemas."""
    from . import blp

    engine = blp._get_engine()
    engine.clear_schema_cache()
    _parse_service_schema_json.cache_clear()
    blp._clear_field_type_resolution_cache()


# IDE configuration
def configure_ide_stubs(
    stubs_dir: Path | str | None = None,
    ide: str | None = None,
) -> str:
    """Configure IDE to recognize xbbg type stubs.

    Only modifies existing config files - does not create new ones.

    Args:
        stubs_dir: Stubs directory (default: ~/.xbbg/stubs/)
        ide: IDE to configure: "vscode", "pyright", "pyproject", "ty", or None for auto-detect

    Returns:
        Message describing what was configured or manual instructions.
    """
    stubs_path = Path.home() / ".xbbg" / "stubs" if stubs_dir is None else Path(stubs_dir)
    stubs_str = str(stubs_path)

    # Find project root (look for common markers)
    cwd = Path.cwd()
    project_root = cwd
    for parent in [cwd, *cwd.parents]:
        if any((parent / marker).exists() for marker in [".git", "pyproject.toml", ".vscode"]):
            project_root = parent
            break

    def _load_toml_module():
        try:
            import tomllib

            return tomllib
        except ImportError:
            try:
                return __import__("tomli")
            except ImportError:
                return None

    toml_module = _load_toml_module()

    def _configure_json_list_setting(
        config_path: Path,
        key: str,
        *,
        indent: int,
        label: str,
    ) -> str | None:
        if not config_path.exists():
            return None
        try:
            config = json.loads(config_path.read_text())
            values = config.get(key, [])
            if stubs_str in values:
                return f"{label} already configured with {stubs_str}"
            values.append(stubs_str)
            config[key] = values
            config_path.write_text(json.dumps(config, indent=indent))
            return f"Configured {label}: added {stubs_str} to {key}"
        except (OSError, json.JSONDecodeError):
            return None

    def _load_toml_config(path: Path) -> tuple[str, dict[str, Any]] | None:
        if toml_module is None or not path.exists():
            return None
        try:
            content = path.read_text()
            parsed = toml_module.loads(content)
            return content, parsed
        except Exception:
            return None

    def _insert_setting(content: str, section_header: str, setting_line: str) -> str | None:
        if section_header not in content:
            return None
        return content.replace(section_header, f"{section_header}\n{setting_line}", 1)

    def _configure_vscode() -> str | None:
        return _configure_json_list_setting(
            project_root / ".vscode" / "settings.json",
            "python.analysis.extraPaths",
            indent=4,
            label="VS Code",
        )

    def _configure_pyright() -> str | None:
        return _configure_json_list_setting(
            project_root / "pyrightconfig.json",
            "extraPaths",
            indent=2,
            label="Pyright",
        )

    def _configure_pyproject_pyright() -> str | None:
        pyproject = project_root / "pyproject.toml"
        loaded = _load_toml_config(pyproject)
        if loaded is None:
            return None

        content, config = loaded
        if "tool" not in config or "pyright" not in config["tool"]:
            return None

        extra_paths = config["tool"]["pyright"].get("extraPaths", [])
        if stubs_str in extra_paths:
            return f"pyproject.toml [tool.pyright] already configured with {stubs_str}"

        if "extraPaths" in content:
            return None

        new_content = _insert_setting(content, "[tool.pyright]", f'extraPaths = ["{stubs_str}"]')
        if new_content is None:
            return None
        pyproject.write_text(new_content)
        return f"Configured pyproject.toml: added {stubs_str} to [tool.pyright].extraPaths"

    def _configure_ty() -> str | None:
        ty_config = project_root / "ty.toml"
        loaded = _load_toml_config(ty_config)
        if loaded is None:
            return None

        content, config = loaded
        extra_paths = config.get("environment", {}).get("extra-paths", [])
        if stubs_str in extra_paths:
            return f"ty.toml already configured with {stubs_str}"

        if "extra-paths" in content:
            return None

        new_content = _insert_setting(content, "[environment]", f'extra-paths = ["{stubs_str}"]')
        if new_content is None:
            return None
        ty_config.write_text(new_content)
        return f"Configured ty.toml: added {stubs_str} to [environment].extra-paths"

    def _configure_pyproject_ty() -> str | None:
        pyproject = project_root / "pyproject.toml"
        loaded = _load_toml_config(pyproject)
        if loaded is None:
            return None

        content, config = loaded
        if "tool" not in config or "ty" not in config["tool"]:
            return None

        extra_paths = config["tool"]["ty"].get("environment", {}).get("extra-paths", [])
        if stubs_str in extra_paths:
            return f"pyproject.toml [tool.ty] already configured with {stubs_str}"

        if "extra-paths" in content:
            return None

        new_content = _insert_setting(content, "[tool.ty.environment]", f'extra-paths = ["{stubs_str}"]')
        if new_content is not None:
            pyproject.write_text(new_content)
            return f"Configured pyproject.toml: added {stubs_str} to [tool.ty.environment].extra-paths"

        new_content = _insert_setting(content, "[tool.ty]", f'environment.extra-paths = ["{stubs_str}"]')
        if new_content is None:
            return None
        pyproject.write_text(new_content)
        return f"Configured pyproject.toml: added {stubs_str} to [tool.ty].environment.extra-paths"

    # Configure specific IDE if requested
    if ide:
        ide_lower = ide.lower()
        if ide_lower == "vscode":
            result = _configure_vscode()
            if result:
                return result
            return "VS Code not configured: .vscode/settings.json not found"
        if ide_lower == "pyright":
            result = _configure_pyright()
            if result:
                return result
            return "Pyright not configured: pyrightconfig.json not found"
        if ide_lower == "pyproject":
            result = _configure_pyproject_pyright()
            if result:
                return result
            return "pyproject.toml not configured: [tool.pyright] section not found"
        if ide_lower == "ty":
            result = _configure_ty() or _configure_pyproject_ty()
            if result:
                return result
            return "ty not configured: ty.toml or [tool.ty] section not found"
        return f"Unknown IDE: {ide}. Use 'vscode', 'pyright', 'pyproject', or 'ty'"

    # Auto-detect: try each in order
    for configure_fn in [
        _configure_vscode,
        _configure_pyright,
        _configure_ty,
        _configure_pyproject_pyright,
        _configure_pyproject_ty,
    ]:
        result = configure_fn()
        if result:
            return result

    # Return manual instructions
    return f"""Stubs generated at: {stubs_str}

To enable IDE autocomplete, add the stubs path to your IDE/type checker config:

VS Code (.vscode/settings.json):
    {{"python.analysis.extraPaths": ["{stubs_str}"]}}

Pyright (pyrightconfig.json):
    {{"extraPaths": ["{stubs_str}"]}}

ty (ty.toml):
    [environment]
    extra-paths = ["{stubs_str}"]

PyCharm:
    Settings > Project > Python Interpreter > Show All > Show paths > Add {stubs_str}
"""


# Stub generation
def generate_stubs(
    service: str,
    output_dir: Path | str | None = None,
) -> str:
    """Generate Python type stubs for a service.

    Creates .pyi files with TypedDict definitions for request/response types.
    Stubs are generated locally for IDE support - never committed to repos.

    Args:
        service: Service URI (e.g., "//blp/refdata")
        output_dir: Output directory (default: ~/.xbbg/stubs/)

    Returns:
        Path to the generated stub file.
    """
    from pathlib import Path as PathClass

    # Get schema
    schema = get_schema(service)

    # Determine output path
    output_dir = PathClass.home() / ".xbbg" / "stubs" if output_dir is None else PathClass(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate stub content
    service_name = schema.service.split("/")[-1]
    stub_content = _generate_stub_content(schema)

    # Write files
    stub_path = output_dir / f"blp_{service_name}.pyi"
    stub_path.write_text(stub_content)

    # Also write .py for runtime imports
    py_path = output_dir / f"blp_{service_name}.py"
    py_path.write_text(stub_content)

    # Configure IDE
    ide_msg = configure_ide_stubs(output_dir)
    print(ide_msg)

    return str(stub_path)


def _generate_stub_content(schema: ServiceSchema) -> str:
    """Generate Python stub content from schema."""
    lines = [
        '"""',
        f"Bloomberg {schema.service} Service Type Stubs",
        "",
        "Auto-generated from runtime schema introspection.",
        "DO NOT EDIT - regenerate using xbbg.schema.generate_stubs()",
        '"""',
        "",
        "from __future__ import annotations",
        "from typing import Any, Dict, List, Literal, Optional, Union",
        "from typing_extensions import TypedDict, NotRequired",
        "import datetime",
        "from decimal import Decimal",
        "",
    ]

    generated_types: set[str] = set()

    # Generate TypedDict for each operation's request
    for op in schema.operations:
        _generate_element_type(lines, op.request, op.name, generated_types)

    return "\n".join(lines)


def _sanitize_name(name: str) -> str:
    """Make a valid Python identifier."""
    name = name.replace("-", "_").replace(".", "_")
    # Handle reserved words
    reserved = {"class", "from", "import", "return", "type", "in", "is", "not", "and", "or"}
    if name in reserved:
        name = f"{name}_"
    return name


def _get_python_type(elem: ElementInfo) -> str:
    """Get Python type annotation for an element."""
    type_map = {
        "Bool": "bool",
        "Char": "str",
        "Byte": "int",
        "Int32": "int",
        "Int64": "int",
        "Float32": "float",
        "Float64": "float",
        "String": "str",
        "Date": "datetime.date",
        "Time": "datetime.time",
        "Decimal": "Decimal",
        "Datetime": "datetime.datetime",
        "Enumeration": "str",
        "ByteArray": "bytes",
        "Name": "str",
        "Sequence": "Dict[str, Any]",
        "Choice": "Any",
    }

    if elem.enum_values:
        # Limit enum values for readability
        values = elem.enum_values[:10]
        values_str = ", ".join(f'"{v}"' for v in values)
        if len(elem.enum_values) > 10:
            values_str += ", ..."
        base_type = f"Literal[{values_str}]"
    else:
        base_type = type_map.get(elem.data_type, "Any")

    if elem.is_array:
        base_type = f"List[{base_type}]"

    return base_type


def _generate_element_type(
    lines: list[str],
    elem: ElementInfo,
    type_name: str,
    generated: set[str],
    depth: int = 0,
) -> None:
    """Generate TypedDict for an element."""
    safe_name = _sanitize_name(type_name)

    if safe_name in generated:
        return
    generated.add(safe_name)

    # Generate nested types first
    for child in elem.children:
        if child.children:
            child_type_name = child.type_name or f"{safe_name}_{child.name}"
            _generate_element_type(lines, child, child_type_name, generated, depth + 1)

    # Generate this type
    lines.append(f"class {safe_name}(TypedDict, total=False):")
    if elem.description:
        lines.append(f'    """{elem.description[:80]}"""')

    if not elem.children:
        lines.append("    pass")
    else:
        for child in elem.children:
            field_name = _sanitize_name(child.name)
            field_type = _get_python_type(child)

            if child.is_optional:
                field_type = f"NotRequired[{field_type}]"

            if child.description:
                desc = child.description[:60].replace("\n", " ")
                lines.append(f"    # {desc}")
            lines.append(f"    {field_name}: {field_type}")

    lines.append("")
