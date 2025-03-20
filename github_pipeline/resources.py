from typing import Any, Generator
from urllib.parse import urljoin

import requests
from datetime import datetime
import re
from typing import Union, Tuple
from dagster import ConfigurableResource, get_dagster_logger


class GitHubAPIResource(ConfigurableResource):
    """Custom Dagster resource for the GitHub REST API.

    Args:
        - github_token (str | None, optional): \
            GitHub token for authentication. If no token is set, the API calls will be without authentication.
        - host (str, optional): \
            Host address of the Contentful Management API. Defaults to 'https://api.github.com'.
    """

    github_token: str | None = None
    """GitHub token for authentication. If no token is set, the API calls will be without authentication."""

    host: str = 'https://api.github.com'
    """Host of the GitHub REST API."""

    def execute_request(
        self,
        method: str,
        path: str,
        params: dict | list[tuple] | None = None,
        json: Any | None = None,
    ) -> requests.Response:
        """Execute a request to the GitHub REST API.

        Args:
            - method (str): \
                HTTP method for the API call, e.g. 'GET'.
            - path (str): \
                Path of the endpoint.
            - params (dict | list[tuple], optional): \
                Dictionary or list of tuples to send as query parameters in the request.
            - json (Any, optional): \
                A JSON serializable Python object to send in the body of the request.

        Returns:
            - requests.Response: \
                Response object of the API call.

        Raises:
            - requests.HTTPError: \
                When HTTP 4xx or 5xx response is received.
        """
        if params is None:
            params = {}
        default_params = {'apiVersion': '2022-11-28'}
        # passed parameters win over the default parameters
        params = {**default_params, **params}

        headers = {'Accept': 'application/vnd.github+json'}
        if self.github_token:
            headers['Authorization'] = f'Bearer {self.github_token}'

        try:
            response = requests.request(
                method=method,
                url=urljoin(self.host, path),
                params=params,
                headers=headers,
                json=json,
            )
            get_dagster_logger().info(f'Call {method}: {response.url}')

            response.raise_for_status()

        except requests.exceptions.HTTPError as err:
            get_dagster_logger().exception(f'{err!r} - {response.text}')

        return response

    def get_metadata(self, owner: str, repo: str) -> dict[str, Any]:
        """Get consolidated metadata about a GitHub repository using main endpoint handler
        and items handler for releases, issues, and PRs. The meatadta
        dictionary is initialized by the main endpoint handler and extended
        with the additional information from the items handler.
        Args:
            - owner (str): \
                The account owner of the repository.
                The name is not case sensitive.
            - repo (str): \
                The name of the repository without the `.git` extension.
                The name is not case sensitive.

        Returns:
            - dict[str, Any]: \
                The metadata for the repository.
        """
        # handle main repo endpoint
        metadata = self.handle_main_repo_endpoint(owner=owner, repo=repo)

        # handle releases
        release_count = self.handle_repo_item(owner=owner,
                                              repo=repo,
                                              suffix='releases')
        metadata['release_count'] = release_count

        # handle closed issues
        closed_issues_count, avg_days_until_issue_was_closed = \
            self.handle_repo_item(owner=owner,
                                  repo=repo,
                                  suffix='issues?state=closed',
                                  calculate_duration=True)
        metadata['closed_issues_count'] = closed_issues_count
        metadata['avg_days_until_issue_was_closed'] = \
            avg_days_until_issue_was_closed

        # handle open prs
        open_pr_count = \
            self.handle_repo_item(owner=owner,
                                  repo=repo,
                                  suffix='pulls?state=open')
        metadata['open_pr_count'] = open_pr_count

        # handle closed prs
        closed_pr_count, avg_days_until_pr_was_closed = \
            self.handle_repo_item(owner=owner,
                                  repo=repo,
                                  suffix='pulls?state=closed',
                                  calculate_duration=True)
        metadata['closed_pr_count'] = closed_pr_count
        metadata['avg_days_until_pr_was_closed'] = \
            avg_days_until_pr_was_closed

        return metadata

    def handle_main_repo_endpoint(self, owner: str, repo: str) \
            -> dict[str, Any]:
        """Get metadata about a GitHub repository using the repo endpoint.
        Docs: https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#get-a-repository

        Args:
            - owner (str): \
                The account owner of the repository.
                The name is not case sensitive.
            - repo (str): \
                The name of the repository without the `.git` extension.
                The name is not case sensitive.

        Returns:
            - dict[str, Any]: \
                The standard metadata for the repository.
        """
        path = f'/repos/{owner}/{repo}'
        response = self.execute_request(method='GET', path=path)
        payload = response.json()
        return payload

    def handle_repo_item(self, owner: str, repo: str, suffix: str,
                         calculate_duration=False) \
            -> Union[int, Tuple[int, float]]:
        """Get the count of items (releases, closed issues, etc.) of a GitHub
        repository using pagination. A while loop is used to iterate over
        the pages until the last page is reached.
        Docs:
        https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28
        https://docs.github.com/en/rest/issues?apiVersion=2022-11-28
        https://docs.github.com/en/rest/pulls?apiVersion=2022-11-28

        Args:
            - owner (str): The account owner of the repository.
                Case insensitive.
            - repo (str): The name of the repository without the .git
                extension. Case insensitive.
            - suffix (str): The API endpoint suffix
                (e.g., 'releases' for releases count,
                'issues?state=closed' for closed issues count,
                'suffix='pulls?state=open' for open pull requests count,
                'suffix='pulls?state=closed' for closed pull requests count).      
            - calculate_duration (bool): If True, calculates the average
                duration in days for closed issues/pull requests.

        Returns:
            - int: Number of items counted (releases, closed issues, closed pull requests, ...)
            - float: Average duration in days for closed issues/pull requests
                if calculate_duration is True.
        """
        # initialize variables
        path = f'/repos/{owner}/{repo}/{suffix}'
        count = 0
        total_duration = 0

        while True:
            # execute request
            response = self.execute_request(method='GET', path=path)
            items = response.json()

            if not items:
                break

            count += len(items)

            if calculate_duration:
                for item in items:
                    created_at = datetime.strptime(item["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                    closed_at = datetime.strptime(item["closed_at"], "%Y-%m-%dT%H:%M:%SZ")
                    total_duration += (closed_at - created_at).days

            # check if there is a next page
            link_attr = response.headers.get('Link')
            if not link_attr:
                break

            # extract the next page URL
            match = re.search(r'<(.*?)>; rel="next"', link_attr)
            if not match:
                break

            path = match.group(1)  # update path for next iteration

        average_duration = total_duration / count if count > 0 else 0

        return (count, average_duration) if calculate_duration else count
