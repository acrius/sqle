from __future__ import annotations

from abc import ABC, abstractclassmethod
from collections.abc import Iterable, Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Protocol, Type, TypeVar

from .exceptions import SerizlierNotFound

if TYPE_CHECKING:
    from .sql import SQLEnvironment


class IterableSpec(str, Enum):
    COLUMNS = "columns"
    VALUES = "values"


class IterableRepresentation:
    def __init__(self, iterable: Iterable, cast: Callable[[Any], Any]) -> None:
        self._original_iterable = iterable
        self._cast = cast

    def __format__(self, __format_spec: str) -> str:
        match __format_spec:
            case IterableSpec.COLUMNS:
                formatted = self.columns_as_string
            case IterableSpec.VALUES:
                formatted = self.values_as_string
            case _:
                formatted = self.iterable_as_sequence(self._original_iterable)

        return formatted

    def __str__(self) -> str:
        return self.iterable_as_sequence(self._original_iterable)

    @cached_property
    def columns_as_string(self) -> str:
        return self.iterable_as_sequence(self.columns)

    @cached_property
    def columns(self) -> list[str]:
        return list(
            sorted(
                {
                    name
                    for item in self._original_iterable
                    for name in self._get_item_column_names(item)
                }
            )
        )

    @staticmethod
    def _get_item_column_names(item: Any) -> list[str]:
        if isinstance(item, Mapping):
            names = list(item)
        elif is_dataclass(item):
            names = [field.name for field in fields(item)]
        else:
            raise ValueError(
                "Only dict and dataclass objects are available for name search."
            )

        return names

    @staticmethod
    def _get_dict_column_names(item: dict) -> list[str]:
        return list(item.keys())

    @cached_property
    def values_as_string(self):
        return ", ".join(
            self.iterable_as_sequence(row_values) for row_values in self.values
        )

    @cached_property
    def values(self) -> list[list[Any]]:
        return [self._get_item_values(item) for item in self._original_iterable]

    def _get_item_values(self, item: Any) -> list[Any]:
        if isinstance(item, Mapping):
            values = [item.get(column_name) for column_name in self.columns]
        elif is_dataclass(item):
            values = [getattr(item, column_name, None) for column_name in self.columns]
        else:
            raise ValueError(
                "Only dict and dataclass objects are available for values search."
            )

        return values

    def iterable_as_sequence(self, iterable) -> str:
        items = ", ".join(self._cast(item) for item in iterable)
        return f"({items})"


class StringRepresentation(Protocol):
    def __str__(self) -> str:
        ...


NO_NAME_PARAM_NAME = "No Name"


class AdapterSerializer:
    def serialize_params(self, values: dict[str, Any]) -> dict[str, Any]:
        return {param: self.serialize_param(value) for param, value in values.items()}

    def serialize_param(self, value: Any, name: str = NO_NAME_PARAM_NAME) -> Any:
        match value:
            case bool():
                _value = "true" if value else "false"
            case int() | float():
                _value = str(value)
            case str():
                _value = self.serialize_string(value)
            case Iterable():
                _value = IterableRepresentation(value, self.serialize_param)
            case None:
                _value = "NULL"
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
