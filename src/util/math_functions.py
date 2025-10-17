import math


def round_down(val, decimals=6):
    """
    Round a number down (towards negative infinity) to a specified number of decimal places.

    Args:
        val (float): The value to round down.
        decimals (int): Number of decimal places to round to. Can be negative to round to powers of ten
                        (e.g., decimals=-1 rounds to the nearest ten, decimals=-2 to the nearest hundred).

    Returns:
        float: The rounded down value.

    Example:
        round_down(123.462, 0)   # 123.0
        round_down(123.462, -1)  # 120.0
        round_down(123.462, -2)  # 100.0
    """
    factor = 10 ** decimals
    return math.floor(val * factor) / factor


def round_up(val, decimals=6):
    """
    Round a number up (towards positive infinity) to a specified number of decimal places.

    Args:
        val (float): The value to round up.
        decimals (int): Number of decimal places to round to. Can be negative to round to powers of ten
                        (e.g., decimals=-1 rounds to the nearest ten, decimals=-2 to the nearest hundred).

    Returns:
        float: The rounded up value.

    Example:
        round_up(123.462, 0)   # 124.0
        round_up(123.462, -1)  # 130.0
        round_up(123.462, -2)  # 200.0
    """
    factor = 10 ** decimals
    return math.ceil(val * factor) / factor
