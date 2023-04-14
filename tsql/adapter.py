from __future__ import annotations

from abc import ABC, abstractclassmethod
from functools import cached_property
from typing import Callable, TYPE_CHECKING, Type, TypeVar, Any, Protocol
from functools import cached_property
from .exceptions import SerizlierNotFound

if TYPE_CHECKING:
    from .sql import SQLEnvironment


class StringRepresentation(Protocol):
    def __str__(self) -> str:
        ...


NO_NAME_PARAM_NAME = "No Name"


class AdapterSerializer:
    def serialize_params(self, values: dict[str, Any]) -> dict[str, Any]:
        return {param: self.serialize_param(value) for param, value in values.items()}

    def serialize_param(self, value: Any, name: str = NO_NAME_PARAM_NAME) -> Any:
        match value:
            case int() | float():
                _value = str(value)
            case str():
                _value = self.serialize_string(value)
            case _:
                _value = self.serialize_other_object(value)

        return _value

    @staticmethod
    def serialize_number(value: int | float) -> str:
        return str(value)

    @staticmethod
    def serialize_string(value: str | StringRepresentation) -> str:
        return f"'{value}'"

    def serialize_other_object(self, value: Any, name: str = NO_NAME_PARAM_NAME) -> str:
        if "__str__" in vars(type(value)):
            _value = self.serialize_string(value)
        else:
            raise SerizlierNotFound(name, type(value))

        return _value


class Adapter(ABC):
    serializer: AdapterSerializer = AdapterSerializer()

    def __init__(
        self,
        sql: SQLEnvironment,
        connection_factory: Callable,
    ) -> None:
        self._sql = sql
        self._connection_factory = connection_factory

    @abstractclassmethod
    def execute(self) -> list[dict[str, Any]]:
        ...

    @cached_property
    def connection(self) -> Any:
        return self._connection_factory()

    @cached_property
    def query(self) -> str:
        return self._sql._query.render(params=self.serialized_params, envs=self.envs)

    @cached_property
    def envs(self) -> dict[str, Any]:
        return {
            **self.adapter_evns,
            **self._sql._envs,
        }

    @property
    def adapter_evns(self) -> dict[str, Any]:
        return {
            "cast": self.serializer.serialize_param,
        }

    @cached_property
    def serialized_params(self) -> dict[str, Any]:
        return self.serializer.serialize_params(self._sql._params)


A = TypeVar("A", bound=Adapter)


class AdapterFactory(ABC):
    adapter_factory: Type[A]

    def __init__(self, connection_factory: Callable) -> None:
        super().__init__()
        self._connection_factory = connection_factory

    @cached_property
    def connection(self) -> Any:
        return self._connection_factory()

    def __get__(
        self,
        sql: SQLEnvironment,
        enviroment: Type[SQLEnvironment] | None = None,
    ) -> A:
        return self.adapter_factory(sql, self._connection_factory)
