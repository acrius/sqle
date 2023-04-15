from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Type

from sqle import Adapter as BaseAdapter
from sqle import AdapterFactory as BaseAdapterFactory
from sqle import AdapterSerializer as BaseAdapterSerializer
from sqle import Query, SQLEnvironment, __version__
from sqle.exceptions import SerizlierNotFound

RESULT_ROWS = [{"name": "test", "email": "test@test.test"}]


class AdapterSerializer(BaseAdapterSerializer):
    def serialize_param(self, value: Any, name: str = ...) -> Any:
        try:
            value = super(AdapterSerializer, self).serialize_param(value, name)
        except SerizlierNotFound:
            if isinstance(value, Iterable):
                _value = [
                    super(AdapterSerializer, self).serialize_param(_value, name)
                    for _value in value
                ]
                _value = ", ".join(_value)
                value = f"({_value})"
            else:
                raise SerizlierNotFound

        return value


class Adapter(BaseAdapter):
    serializer: AdapterSerializer = AdapterSerializer()

    def execute(self) -> list[dict[str, Any]]:
        return RESULT_ROWS


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
    where profile_id in {profile_ids}
    {% if use_pagination %}limit {limit} offset {offset}{% endif %};
    """
)

TARGET_SQL_WITH_PAGINATION = """
select profile_id, name from profiles
where profile_id in (1, 2, 3)
limit 20 offset 0;
"""

TARGET_SQL_WITHOUT_PAGINATION = """
select profile_id, name from profiles
where profile_id in (1, 2, 3)
;
"""


def test_sql():
    sql = SQL.create_instance(adapter=create_connection)

    _sql = sql(SQL_TEMPLATE)

    assert isinstance(_sql.adapter, Adapter)

    adapter = _sql.with_params(
        profile_ids=[1, 2, 3],
    ).adapter

    assert adapter.query == TARGET_SQL_WITHOUT_PAGINATION

    assert adapter.connection == test_connection

    assert adapter.execute() == RESULT_ROWS


def test_version():
    assert __version__ == "0.0.0a0"
