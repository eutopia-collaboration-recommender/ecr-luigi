import re


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
