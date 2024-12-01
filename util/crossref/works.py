import backoff
import requests


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, requests.exceptions.HTTPError),
    max_tries=8,
    giveup=lambda e: e.response is not None and e.response.status_code < 500,
)
def crossref_request(url: str, params: dict = dict()) -> dict:
    """
    Make a request to the given URL with the given parameters
    :param url: URL to make the request to.
    :param params: Parameters to include in the request.
    :return: JSON response from the request.
    """
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        response_json = response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}")
        response_json = {}

    return response_json
