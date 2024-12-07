import json
import time

import requests
from requests.exceptions import ChunkedEncodingError


def request_orcid(url: str, access_token: str) -> requests.Response:
    """
    Request call on ORCID API
    :param url: URL
    :param access_token: ORCID access token
    :return: JSON response
    """
    # Define headers
    headers_record: dict = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    # Put the current time in the queue
    try:
        # Fetch data from ORCID API
        response = requests.get(url=url, headers=headers_record)
    except ChunkedEncodingError:
        print(f"ChunkedEncodingError occurred. URL returned an error: {url}.")
        raise ChunkedEncodingError

    return response


def search_modified_records(access_token: str,
                            affiliation: str,
                            updated_date_start: str = time.strftime(
                                "%Y-%m-%dT%H:%M:%SZ",
                                time.gmtime(time.time() - 7 * 24 * 60 * 60)
                            ),
                            updated_date_end: str = 'NOW',
                            start: int = 0,
                            num_rows: int = 1000) -> dict:
    """
    Search for modified records in ORCID for a specific affiliation
    :param access_token: ORCID access token
    :param affiliation: Affiliation name
    :param updated_date_start: Start date for the search
    :param updated_date_end: End date for the search
    :return: DataFrame with the modified records
    :param start: Start index
    :param num_rows: Number of rows to fetch
    """

    # Define parameters that will be propagated to the query
    params = {
        'profile-last-modified-date': f'%5B{updated_date_start}%20TO%20{updated_date_end}%5D',
        'affiliation-org-name': f'"{affiliation}"'
    }
    query = ' AND '.join([f'{param_key}:{param_value}' for param_key, param_value in params.items()])

    _url = f"https://pub.orcid.org/v3.0/search/?q={query}&start={start}&rows={num_rows}"

    # Fetch data from ORCID API
    _response = request_orcid(url=_url,
                              access_token=access_token)

    # Handle the response
    if _response.status_code == 200:
        return _response.json()
    else:
        print(f"Failed to fetch data: {_response.status_code}, {_response.text}")
    return _response.json()


def get_orcid_member_works(member_id: str, access_token: str) -> dict:
    """
    Fetch the ORCID record by ORCID ID
    :param member_id: ORCID identifier
    :param access_token: ORCID access token
    :return: ORCID record
    """
    # Fetch the ORCID record
    response = request_orcid(
        url=f"https://pub.orcid.org/v3.0/{member_id}/works",
        access_token=access_token,
    )

    # Define the record
    record = dict(member_id=member_id, member_works=response.json()['group'])

    # Return the record from ORCID
    return record


def get_orcid_member_person(member_id: str, access_token: str) -> dict:
    """
    Fetch the ORCID record by ORCID ID
    :param member_id: ORCID identifier
    :param access_token: ORCID access token
    :return: ORCID record
    """
    # Fetch the ORCID record
    response = request_orcid(
        url=f"https://pub.orcid.org/v3.0/{member_id}/person",
        access_token=access_token,
    )

    # Define the record
    record = dict(member_id=member_id, member_person=json.dumps(response.json()))

    # Return the record from ORCID
    return record


def get_orcid_member_employments(member_id: str, access_token: str) -> dict:
    """
    Fetch the ORCID record by ORCID ID
    :param member_id: ORCID identifier
    :param access_token: ORCID access token
    :return: ORCID record
    """
    # Fetch the ORCID record
    response = request_orcid(
        url=f"https://pub.orcid.org/v3.0/{member_id}/employments",
        access_token=access_token,
    )

    # Define the record
    record = dict(member_id=member_id, member_employments=json.dumps(response.json().get('affiliation-group', [])))

    # Return the record from ORCID
    return record
