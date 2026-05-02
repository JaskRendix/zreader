import pytest

from app.core.filters import (
    NDJSONFilter,
    field_equals,
    field_exists,
    field_in,
    field_not_exists,
    numeric_range,
)


def test_field_equals_filter():
    f = NDJSONFilter()
    f.add(field_equals("subreddit", "python"))

    assert f.match({"subreddit": "python"}) is True
    assert f.match({"subreddit": "cats"}) is False
    assert f.match({}) is False


def test_field_in_filter():
    f = NDJSONFilter([field_in("tag", ["a", "b", "c"])])

    assert f.match({"tag": "a"}) is True
    assert f.match({"tag": "c"}) is True
    assert f.match({"tag": "z"}) is False
    assert f.match({}) is False


def test_field_exists_filter():
    f = NDJSONFilter([field_exists("x")])

    assert f.match({"x": 1}) is True
    assert f.match({"x": None}) is True
    assert f.match({"y": 2}) is False


def test_field_not_exists_filter():
    f = NDJSONFilter([field_not_exists("secret")])

    assert f.match({"a": 1}) is True
    assert f.match({"secret": 123}) is False


def test_numeric_range_min_only():
    f = NDJSONFilter([numeric_range("score", min_val=10)])

    assert f.match({"score": 10}) is True
    assert f.match({"score": 11}) is True
    assert f.match({"score": 9}) is False
    assert f.match({}) is False


def test_numeric_range_max_only():
    f = NDJSONFilter([numeric_range("score", max_val=5)])

    assert f.match({"score": 5}) is True
    assert f.match({"score": 1}) is True
    assert f.match({"score": 6}) is False


def test_numeric_range_both_bounds():
    f = NDJSONFilter([numeric_range("score", min_val=1, max_val=3)])

    assert f.match({"score": 1}) is True
    assert f.match({"score": 2}) is True
    assert f.match({"score": 3}) is True
    assert f.match({"score": 0}) is False
    assert f.match({"score": 4}) is False


def test_numeric_range_rejects_non_numeric():
    f = NDJSONFilter([numeric_range("score", min_val=0, max_val=10)])

    assert f.match({"score": "x"}) is False
    assert f.match({"score": None}) is False
    assert f.match({"score": {}}) is False


@pytest.mark.asyncio
async def test_filter_stream_async():
    async def gen():
        yield {"x": 1}
        yield {"x": 2}
        yield {"x": 3}

    f = NDJSONFilter([numeric_range("x", min_val=2)])
    out = [o async for o in f.filter_stream(gen())]

    assert out == [{"x": 2}, {"x": 3}]


def test_filter_multiple_predicates_all_pass():
    f = NDJSONFilter(
        [
            field_equals("type", "post"),
            field_in("subreddit", ["python", "cats"]),
            numeric_range("score", min_val=10),
        ]
    )

    obj = {"type": "post", "subreddit": "python", "score": 42}
    assert f.match(obj) is True


def test_filter_multiple_predicates_one_fails():
    f = NDJSONFilter(
        [
            field_equals("type", "post"),
            field_in("subreddit", ["python", "cats"]),
            numeric_range("score", min_val=10),
        ]
    )

    obj = {"type": "post", "subreddit": "python", "score": 5}
    assert f.match(obj) is False


def test_filter_predicate_order_does_not_matter():
    f1 = NDJSONFilter(
        [
            field_exists("x"),
            numeric_range("x", min_val=5),
        ]
    )

    f2 = NDJSONFilter(
        [
            numeric_range("x", min_val=5),
            field_exists("x"),
        ]
    )

    obj = {"x": 10}
    assert f1.match(obj) is True
    assert f2.match(obj) is True


def test_filter_combination_missing_field_rejected():
    f = NDJSONFilter(
        [
            field_exists("x"),
            numeric_range("x", min_val=0),
        ]
    )

    assert f.match({}) is False
    assert f.match({"y": 1}) is False


def test_filter_large_predicate_chain():
    preds = [numeric_range("v", min_val=i) for i in range(10)]
    f = NDJSONFilter(preds)

    assert f.match({"v": 20}) is True
    assert f.match({"v": 5}) is False
