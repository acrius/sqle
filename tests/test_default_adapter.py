from __future__ import annotations

from random import randint, uniform
from typing import Any, Type

from sqle import Adapter as BaseAdapter
from sqle import AdapterFactory as BaseAdapterFactory
from sqle import AdapterSerializer, Query, SQLEnvironment


class Adapter(BaseAdapter):
    serializer: AdapterSerializer = AdapterSerializer()

    def execute(self) -> list[dict[str, Any]]:
        ...


class AdapterFactory(BaseAdapterFactory):
    adapter_factory: Type[Adapter] = Adapter


class SQL(SQLEnvironment):
    adapter: AdapterFactory


class TestConnection:
    ...


test_connection = TestConnection()


def create_connection():
    return test_connection


SQL_TEMPLATE = Query(
    """
    select profile_id, name from profiles
    where age = {age} and name = {name} and score = {score}
    and is_active = {is_active} and group = {group};
    """
)

TEST_NAME = "Egor"


def test_default_adapter_serializer():
    sql = SQL.create_instance(adapter=create_connection)

    target_query_string = SQL_TEMPLATE._text

    age = randint(-150, 150)
    score = uniform(-100.0, 100.0)
    is_active = bool(randint(0, 1))

    target_query = "\n".join(
        row.strip()
        for row in target_query_string.format(
            age=str(age),
            name=f"'{TEST_NAME}'",
            score=str(score),
            is_active="true" if is_active else "false",
            group="NULL",
        ).split("\n")
    )

    _sql = sql(SQL_TEMPLATE)

    adapter = _sql.with_params(
        age=age,
        name=TEST_NAME,
        score=score,
        is_active=is_active,
        group=None,
    ).adapter

    assert adapter.query == target_query
