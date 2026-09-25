from __future__ import annotations

from typing import Any, Iterable


class ConnectorRegistry:
    """Dispatch table keyed by canonical connector name."""

    def __init__(self, connectors: Iterable[Any] = ()) -> None:
        self._connectors: dict[str, Any] = {}
        for connector in connectors:
            self.register(connector)

    def register(self, connector: Any) -> None:
        name = str(getattr(connector, "name", "") or "").strip()
        if not name:
            raise ValueError("A connector must declare a non-empty name")
        if name in self._connectors:
            raise ValueError(f"Duplicate connector name: {name}")
        self._connectors[name] = connector

    def get(self, name: str) -> Any | None:
        return self._connectors.get(name)

    @property
    def names(self) -> list[str]:
        return sorted(self._connectors)

    def __contains__(self, name: object) -> bool:
        return name in self._connectors

    def __len__(self) -> int:
        return len(self._connectors)


def default_registry() -> ConnectorRegistry:
    """Production registry of rights-cleared external connectors.

    No external connector has passed rights + exact-entity review yet, so the
    production registry is deliberately empty. Real connectors are added here as
    they clear review; test-only connectors are never registered in this path.
    """
    return ConnectorRegistry()