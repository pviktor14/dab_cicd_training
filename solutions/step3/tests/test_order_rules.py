"""Unit tests for src/order_rules.py.

CI runs them with:      pytest            (from the bundle folder, see pytest.ini)
In the workspace:       open tests/run_tests and click Run all
"""

from order_rules import is_valid_amount, normalize_country, order_size


def test_normalize_country_cleans_whitespace_and_case():
    assert normalize_country(" de ") == "DE"
    assert normalize_country("at") == "AT"


def test_normalize_country_unknown_values():
    assert normalize_country(None) == "UNKNOWN"
    assert normalize_country("") == "UNKNOWN"
    assert normalize_country("XX") == "UNKNOWN"


def test_is_valid_amount():
    assert is_valid_amount(10.0)
    assert not is_valid_amount(0)
    assert not is_valid_amount(-5)
    assert not is_valid_amount(None)


def test_order_size_boundaries():
    assert order_size(49.99) == "small"
    assert order_size(50) == "medium"
    assert order_size(999.99) == "medium"   # changed in lab step 3 (was 499.99)
    assert order_size(1000) == "large"      # changed in lab step 3 (was 500)


def test_order_size_rejects_invalid_amounts():
    # (plain Python instead of pytest.raises -> also runs in the workspace without pytest)
    try:
        order_size(-1)
    except ValueError:
        return
    raise AssertionError("order_size(-1) should raise ValueError")
