from __future__ import annotations

from collections.abc import Mapping
from inspect import isclass, stack
from os.path import isabs
from pathlib import Path
from typing import Callable, Type, TypeVar, Any

from jinja2 import Environment
from typing_extensions import get_type_hints

from .adapter import AdapterFactory
from .contrib.class_property import classproperty
from .exceptions import TSQLValueError
from .query import Query
from .settings import SQL_ENVIRONMENT_DEFAULT_INSTANCE_NAME

T = TypeVar("T", bound="SQLEnvironment")


class SQLEnvironment:
    def __init__(self, query: Query | str) -> None:
        self._query = query if isinstance(query, Query) else Query(query)
        self._envs = {}
        self._params = {}

    def with_params(self, **params) -> SQLEnvironment:
        return self.copy(with_params=params)

    def with_envs(self, **envs) -> SQLEnvironment:
        return self.copy(with_envs=envs)

    def copy(
        self: T,
        with_envs: dict | None = None,
        with_params: dict | None = None,
    ) -> T:
        sql = self.__class__(self._query)

        if with_envs:
            if not isinstance(with_envs, Mapping):
                raise TSQLValueError(f"Envs must be `Mapping`, got {type(with_envs)}.")

            sql._extend_envs(**with_envs)

        if with_params:
            if not isinstance(with_params, Mapping):
                raise TSQLValueError(
                    f"Params nust be `Mapping`, get {type(with_params)}."
                )

            sql._extends_params(**with_params)

        return sql

    def _extend_envs(self, **envs: Any):
        self._envs = {
            **self._envs,
            **envs,
        }

    def _extends_params(self, **params: Any):
        self._params = {
            **self._params,
            **params,
        }

    # Factory

    @classmethod
    def create_instance(
        cls,
        name: str = SQL_ENVIRONMENT_DEFAULT_INSTANCE_NAME,
        **connection_factories: Callable,
    ) -> Type[SQLEnvironment]:
        sql_environment_instance = type(name, cls.__bases__, dict(cls.__dict__))

        for adapter_name, adapter_factory in cls.adapters.items():
            connection_factory = connection_factories.get(adapter_name)

            if not connection_factory and not cls.get_default_adapter(adapter_name):
                raise TSQLValueError(
                    f"Pass the connection factory or set the default adapter for {adapter_name}({adapter_factory}).",
                )

            if connection_factory:
                setattr(
                    sql_environment_instance,
                    adapter_name,
                    adapter_factory(connection_factory),
                )

        return sql_environment_instance

    @classmethod
    def get_default_adapter(cls, adapter_name: str) -> Adapter | None:
        return getattr(cls, adapter_name, None)

    @classproperty
    def adapters(cls) -> dict[str, type[AdapterFactory]]:
        return {
            field: type_
            for field, type_ in get_type_hints(cls).items()
            if isclass(type_) and issubclass(type_, AdapterFactory)
        }
