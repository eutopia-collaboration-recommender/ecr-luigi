import requests


def get_access_token(client_id: str, client_secret: str) -> str:
    """
    Fetch the access token from ORCID (based on the client ID and client secret that are stored in Secret Manager)
    :param client_secret: Client secret
    :param client_id: Client ID
    :return: Access token
    """

    # Define headers
    headers_token: dict = {"Accept": "application/json"}

    # Define URL
    url_token: str = "https://orcid.org/oauth/token"

    # Define data
    data_token: dict = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "/read-public",
    }

    # POST request
    response_token: requests.Response = requests.post(
        url=url_token, headers=headers_token, data=data_token
    )
    # Get access from response
    access_token = response_token.json().get("access_token")

    # Return the access token
    return access_token
