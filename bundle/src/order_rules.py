"""Business rules for Danube Goods orders.

Pure Python on purpose: no Spark, no Databricks. That is what makes these rules
unit-testable in seconds - on a laptop, in the workspace, or in a CI pipeline
(see ../tests/test_order_rules.py).
"""

# Orders with an amount >= this value count as "large".
LARGE_ORDER_THRESHOLD = 500

# Orders with an amount < this value count as "small".
SMALL_ORDER_THRESHOLD = 50

KNOWN_COUNTRIES = {"AT", "DE", "HU", "SK", "CZ"}


def normalize_country(code):
    """' de ' -> 'DE'.  None, '' or unknown codes -> 'UNKNOWN'."""
    if code is None:
        return "UNKNOWN"
    cleaned = str(code).strip().upper()
    return cleaned if cleaned in KNOWN_COUNTRIES else "UNKNOWN"


def is_valid_amount(amount):
    """An order amount must be present and positive."""
    return amount is not None and amount > 0


def order_size(amount):
    """Classify an order amount as 'small', 'medium' or 'large'."""
    if not is_valid_amount(amount):
        raise ValueError(f"invalid amount: {amount!r}")
    if amount < SMALL_ORDER_THRESHOLD:
        return "small"
    if amount < LARGE_ORDER_THRESHOLD:
        return "medium"
    return "large"
