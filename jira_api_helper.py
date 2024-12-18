import os
import sys
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
import urllib.parse
import json
import time
from datetime import datetime, timezone
from dateutil import parser
import pytz


def jira_api_query(base_url: str,
                   issue_types_list: list,
                   query_duration: int,
                   query_units: str,
                   jira_auth: HTTPBasicAuth,
                   fields_list: list = ['key', 'status', 'resolution', 'issuetype', 'summary', 'assignee', 'created', 'updated', 'parent']
                   ) -> tuple | None:
    """
    Performs an API get request with JQL query.
    Returns a tuple containing a list of issues and the query start time (US/Eastern).
    :param base_url:  URL of Jira API with a formatted JQL query
    :param issue_types_list: List of issue types to be included in JQL query.
    :param query_duration: Time range to include in JQL query. Specify integer value here and units in query_units
        ex.: '15m'=15 minutes; '24h'=24 hours; '2w'=2 weeks
    :param query_units: unit of time for query_duration
    :param jira_auth:  HTTPBasicAuth object
    :param fields_list: List of fields to retrieve in query.
    :return:  Tuple with response object, list of issues data, and query start timestamp (US/Eastern to match Jira system time) or None.
    """
    issue_types_str = "','".join(issue_types_list)
    jql_query = f"""(created>=-{str(query_duration)}{query_units} OR updated>=-{str(query_duration)}{query_units}) AND issuetype IN ('{issue_types_str}')"""
    jql_query_parsed = urllib.parse.quote(jql_query)
    url = base_url + "/search?jql=" + jql_query_parsed
    start_at = 0
    total = float('inf')
    issue_response = []
    while start_at <= total:
        time_start = time.time()
        time_start_est = datetime.now(pytz.timezone('US/Eastern')).timestamp()
        response = requests.request(
            method="GET",
            url=url,
            auth=jira_auth,
            headers={"Accept": "application/json"},
            params={
                'fields': fields_list,
                'fieldsByKeys': 'false',
                'expand': 'changelog',
                'startAt': start_at,
                'maxResults': '100'
            }
        )
        if response.status_code == 429:
            print("API Rate Limiting")
            if 'Retry-After' in response.headers.keys():
                wait = response.headers['Retry-After']
                print("Retry-After: " + str(wait))
            if 'X-RateLimit-Reset' in response.headers.keys():
                reset_time = parser.parse(response.headers['X-RateLimit-Reset'])
                now = datetime.now(timezone.utc)
                wait = (reset_time - now).total_seconds() + 120
                print('X-RateLimit-Reset:' + response.headers['X-RateLimit-Reset'])
            time.sleep(wait)
        elif response.status_code != 200:
            print(f"Response status code: {response.status_code}\n"
                  f"The request returned the following error:\n{response.text}")
            raise RequestException
        else:
            total = json.loads(response.content)['total']
            if total == 0:
                response = None
                issue_response = None
                time_start_est = time_start_est
                return response, issue_response, time_start_est
        start_at = start_at + 100
        issue_response = issue_response + json.loads(response.content)['issues']
    return response, issue_response, time_start_est


def check_created_date(record: str, start: float, query_duration: int, query_units: str) -> bool:
    """Compares query date range with changelog created date (string).
    Returns True if changelog was created in query date range."""
    if query_units == 'm':
        check = start - datetime.strptime(record, '%Y-%m-%dT%H:%M:%S.%f%z').timestamp() <= query_duration * 60
    elif query_units == 'h':
        check = start - datetime.strptime(record, '%Y-%m-%dT%H:%M:%S.%f%z').timestamp() <= query_duration * 60 * 60
    else:
        print("check_created_date() requires query units as either 'm' or 'h'")
        raise Exception
    return check


# Testing
if __name__ == '__main__':
    jira_url = "https://ossovr.atlassian.net/rest/api/2"
    duration = 24
    units = 'h'
    issue_types = ['Story', 'Task', 'Bug', 'Epic', 'Initiative', 'Company Portfolio']
    with open("../creds.json", "r") as f:
        f_data = json.load(f)
        auth = HTTPBasicAuth(
            username=f_data[0]['jira_username'],
            password=f_data[0]['jira_password']
        )
    api_response, issues, start_est = jira_api_query(
        base_url=jira_url,
        issue_types_list=issue_types,
        query_duration=duration,
        query_units=units,
        jira_auth=auth
    )


