from app.core.filters import NDJSONFilter, field_equals


def test_field_equals_filter():
    f = NDJSONFilter()
    f.add(field_equals("subreddit", "python"))

    assert f.match({"subreddit": "python"}) is True
    assert f.match({"subreddit": "cats"}) is False
    assert f.match({}) is False
