import backoff
import requests
import json


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


def query_top_n_by_keyword(base_url: str,
                           params: dict,
                           keyword: str,
                           n: int) -> list:
    """
    Query the top N DOIs by keyword concatenated to a string to be input into the text embedding model.
    :param params:
    :param base_url:
    :param keyword: Keyword to search for.
    :param n: Number of DOIs to return.
    :return: List of DOIs.
    """

    # Query the top N by keyword, sorted by relevance, only select title and abstract
    url = f"{base_url}?query={keyword}&sort=relevance&rows={n}&filter=has-abstract:true"
    response = crossref_request(url, params)
    return [dict(
        publication_doi=article['DOI'],
        publication_metadata=json.dumps(article)
    ) for article in response.get("message")['items']][:n]


def query_keyword_trend(base_url: str,
                        params: dict,
                        keyword: str,
                        start_year: int,
                        end_year: int) -> list:
    trend_data = []
    for year in range(start_year, end_year + 1):
        params['query'] = keyword
        params['filter'] = f"from-pub-date:{year},until-pub-date:{year}"
        params['rows'] = 0  # Retrieve only metadata count, not detailed data

        response = crossref_request(base_url, params)

        if response['status'] == 'ok':
            total_results = response["message"]["total-results"]
            trend_data.append({"article_keyword": keyword, "year": year, "publication_count": total_results})
        else:
            print(f"Error fetching data for {year}: {response['status']}")
            trend_data.append({"article_keyword": keyword, "year": year, "publication_count": None})

    return trend_data
