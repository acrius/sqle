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


SELECT_SQL_QUERY_TEMPLATE = Query(
    """
    select profile_id, name from profiles
    where age = {age} and name = {name} and score = {score}
    and is_active = {is_active} and group = {group};
    """
)

TEST_NAME = "Egor"


def test_select_query_default_adapter_serializer():
    sql = SQL.create_instance(adapter=create_connection)

    target_query_string = SELECT_SQL_QUERY_TEMPLATE._text

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

    _sql = sql(SELECT_SQL_QUERY_TEMPLATE)

    adapter = _sql.with_params(
        age=age,
        name=TEST_NAME,
        score=score,
        is_active=is_active,
        group=None,
    ).adapter

    assert adapter.query == target_query


INSERT_SQL_TEST_QUERY_TEMPLATE = Query(
    """
    insert into profiles {profiles:columns}
    values {profiles:values};
    """
)


INSERT_PROFILES_TEST_DATA = [
    {
        "name": "Wang Miao",
        "age": 41,
        "is_active": True,
    },
    {
        "name": "Ye Zhetai",
        "age": 64,
        "is_active": False,
    },
]

INSERT_SQL_TEST_RESULT_QUERY = """
    insert into profiles ('age', 'is_active', 'name')
    values (41, true, 'Wang Miao'), (64, false, 'Ye Zhetai');
"""


def test_insert_query_default_adapter_serializer():
    sql = SQL.create_instance(adapter=create_connection)
    adapter = (
        sql(INSERT_SQL_TEST_QUERY_TEMPLATE)
        .with_params(profiles=INSERT_PROFILES_TEST_DATA)
        .adapter
    )

    target_query = "\n".join(
        row.strip() for row in INSERT_SQL_TEST_RESULT_QUERY.split("\n")
    )

    assert adapter.query == target_query
