from __future__ import annotations

from sqle import Query

TEST_TARGET_QUERY = """
    select * from profiles
    limit {limit} offset {offset};
"""

TARGET_LIMIT = 10
TARGET_OFFSET = 0

RENDERED_TARGET_QUERY = TEST_TARGET_QUERY.format(
    limit=TARGET_LIMIT,
    offset=TARGET_OFFSET,
)


def test_query_from_string():
    query = Query(TEST_TARGET_QUERY)
    renderd_query = query.render(
        params={"limit": TARGET_LIMIT, "offset": TARGET_OFFSET}
    )

    assert renderd_query == "\n".join(
        [line.strip() for line in RENDERED_TARGET_QUERY.split("\n")]
    )


def test_query_from_file_with_macro():
    query = Query.from_file("select_profiles_with_macro.sql")
    rendered_query = query.render(
        params={"limit": TARGET_LIMIT, "offset": TARGET_OFFSET},
    )

    assert rendered_query == "\n".join(
        [line.strip() for line in RENDERED_TARGET_QUERY.split("\n")]
    )


def test_query_from_file_with_include():
    query = Query.from_file("select_profiles_with_include.sql")
    rendered_query = query.render(
        params={"limit": TARGET_LIMIT, "offset": TARGET_OFFSET},
    )

    assert rendered_query == "\n".join(
        [line.strip() for line in RENDERED_TARGET_QUERY.split("\n")]
    )
