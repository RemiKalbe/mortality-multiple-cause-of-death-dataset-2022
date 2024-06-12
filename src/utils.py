from resulty import Ok, Err, Result
from typing import Optional


def maybe_del[K, V](d: dict[K, V], key: K) -> None:
    """
    Removes the specified key from the dictionary if it exists.

    Args:
        d (dict): The dictionary to remove the key from.
        key: The key to be removed.

    Returns:
        None
    """
    try:
        del d[key]
    except KeyError:
        pass


def try_parse_int(s: str) -> Result[int, str]:
    """
    Tries to parse a string as an integer.

    Args:
      s (str): The string to be parsed.

    Returns:
      Result[int, str]: A Result object containing the parsed integer or an error message.

    """
    try:
        i = int(s)
        return Ok(i)
    except ValueError:
        return Err(f"Could not parse '{s}' as an integer.")


type RangeKeyedDict[V] = dict[tuple[int, int], V]


def get_in_range[V](d: RangeKeyedDict[V], v: int) -> Optional[V]:
    """
    Gets the value from a dictionary that corresponds to a given value within a range.

    Args:
      d (dict[tuple[int, int], T]): The dictionary to search.
      v (int): The value to search for.

    Returns:
      Optional[T]: The value corresponding to the given value, or None if no such value exists.

    """
    for (start, end), value in d.items():
        if start <= v <= end:
            return value
    return None


def range_keyed_dict_has_key[V](d: RangeKeyedDict[V], v: int) -> bool:
    """
    Checks if the given value is within the range of any key in the dictionary.

    Args:
      d (dict[tuple[int, int], T]): The dictionary to search.
      v (int): The value to search for.

    Returns:
      bool: True if the value is within the range of any key, False otherwise.

    """
    return any(start <= v <= end for (start, end) in d.keys())
