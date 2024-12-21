import re


def element_in_flattened_list(element: str,
                              list_of_lists: list) -> bool:
    """
    Check if an element is in a list of lists.
    :param element: Element to check.
    :param list_of_lists: List of lists to check.
    :return: True if the element is in the list of lists, False otherwise.
    """
    return any(element in sublist for sublist in list_of_lists)


def to_snake_case(_string: str):
    """
    Convert a string to snake case
    :param _string: String to convert
    :return: Converted string in snake case
    """
    # Insert a space before a capital letter if preceded by a lowercase letter
    _string = re.sub(r'(?<=[a-z])(?=[A-Z])|[^a-zA-Z0-9]', ' ', _string)
    # Replace spaces with underscores and convert to lowercase
    _string = _string.strip().replace(' ', '_').lower()
    return _string
