import random
import string
from datetime import datetime, timedelta

def get_random_string(length: int, prefix: str = '') -> str:
    return prefix + ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def get_random_int(min: int = 0, max: int = 100000) -> int:
    return random.randint(min, max)

def get_random_index_string(length: int = 10, prefix: str = '') -> str:
    if length <= 1:
        return prefix + str(random.choice(string.digits[1:]))
    else:
        return prefix + ''.join(random.choices(string.digits[1:], k=1)) + ''.join(random.choices(string.digits,k=(length-1)))

def get_random_float(min: float, max: float,decimal_point: int = 2) -> float:
    return round(random.uniform(min, max), decimal_point)

def get_random_boolean() -> bool:
    return random.choice([True,False])

def get_random_string_from_item(items: list) -> str:
    return random.choice(items)

def get_random_timestamp() -> datetime:
    return datetime.now() + timedelta(days=random.randint(0, 365))

