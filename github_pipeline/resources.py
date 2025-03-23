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
        """Get consolidated metadata about a GitHub repository using
        main endpoint handler, releases handler, and issues handler.
        Releases handler and issues handler use a generic method that
        uses pagination to get all items as a flattened list.
        Metadata is initialized with the main endpoint handler output and
        extended with the results of the releases and issues handlers.
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
        release_count = self.handle_releases(owner=owner,
                                             repo=repo)
        metadata['release_count'] = release_count

        # handle issues (which contain issues and PRs)
        (open_issues, closed_issues, open_prs, closed_prs,
            average_issue_duration, average_pr_duration) = \
            self.handle_issues(owner=owner, repo=repo)
        metadata['open_issues'] = open_issues
        metadata['closed_issues'] = closed_issues
        metadata['open_prs'] = open_prs
        metadata['closed_prs'] = closed_prs
        metadata['average_issue_duration'] = average_issue_duration
        metadata['average_pr_duration'] = average_pr_duration

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

    def handle_repo_item(self, owner: str, repo: str,
                         suffix: str) \
            -> list[dict[str, Any]]:
        """Get the items (releases, issues, etc.) of a GitHub
        repository using pagination. A while loop is used to iterate over
        the pages until the last page is reached. The flattened list of items
        is returned.
        Docs:
        https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28
        https://docs.github.com/en/rest/issues?apiVersion=2022-11-28

        Args:
            - owner (str): \
                The account owner of the repository.
                The name is not case sensitive.
            - repo (str): \
                The name of the repository without the `.git` extension.
                The name is not case sensitive.

        Returns:
            - list[dict[str, Any]]: \
                Flattened list of items from all pages.
        """

        # initialize variables
        path = f'/repos/{owner}/{repo}/{suffix}'
        nested_list = []
        while True:
            # execute request
            response = self.execute_request(method='GET', path=path)
            items = response.json()

            if not items:
                break
            nested_list.append(items)

            # check if there is a next page
            link_attr = response.headers.get('Link')
            if not link_attr:
                break

            # extract the next page URL
            match = re.search(r'<(.*?)>; rel="next"', link_attr)
            if not match:
                break

            path = match.group(1)  # update path for next iteration

        flattened_list = [item for sublist in nested_list for item in sublist]
        return flattened_list

    def handle_releases(self, owner: str, repo: str) -> int:
        """Get the count of releases of a GitHub repository
        using the generic method `handle_repo_item`.
        Length of the list of releases is returned.
        Docs:
        https://docs.github.com/en/rest/repos/releases?apiVersion=2022-11-28

        Args:
            - owner (str): \
                The account owner of the repository.
                The name is not case sensitive.
            - repo (str): \
                The name of the repository without the `.git` extension.
                The name is not case sensitive.
        Returns:
            - int: Number of releases counted.

        """
        releases = self.handle_repo_item(owner=owner, repo=repo,
                                         suffix='releases')
        return len(releases)

    def handle_issues(self, owner: str, repo: str) \
            -> Tuple[int, int, int, int, float, float]:
        """Get the count of open and closed issues and pull requests
        and the average duration for closed issues and pull requests of
        a GitHub repository using the generic method `handle_repo_item`.
        Afterward, the method iterates over all issues
        and calculates counts and durations.
        'state=all' needs to be used to get all issues,
        otherwise only open issues are returned.
        Docs:
        https://docs.github.com/en/rest/issues?apiVersion=2022-11-28
        """
        # get all issues
        issues = self.handle_repo_item(owner=owner, repo=repo,
                                       suffix='issues?state=all')
        # initialize variables
        open_issues = 0
        closed_issues = 0
        open_prs = 0
        closed_prs = 0
        issues_duration = 0
        prs_duration = 0

        # iterate over issues
        for issue in issues:
            if issue['state'] == 'open':  # open issue / pr
                if 'pull_request' in issue.keys():  # open pull request
                    open_prs += 1
                else:  # open issue
                    open_issues += 1
            elif issue['state'] == 'closed':  # closed issue / pr
                if 'pull_request' in issue.keys():  # closed pull request
                    closed_prs += 1
                    created_at = datetime.strptime(issue['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                    closed_at = datetime.strptime(issue['closed_at'], '%Y-%m-%dT%H:%M:%SZ')
                    prs_duration += (closed_at - created_at).days
                else:  # closed issue
                    closed_issues += 1
                    created_at = datetime.strptime(issue['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                    closed_at = datetime.strptime(issue['closed_at'], '%Y-%m-%dT%H:%M:%SZ')
                    issues_duration += (closed_at - created_at).days

        average_pr_duration = \
            prs_duration / closed_prs if closed_prs > 0 else 0
        average_issue_duration = \
            issues_duration / closed_issues if closed_issues > 0 else 0
        return (open_issues, closed_issues, open_prs, closed_prs,
                average_issue_duration, average_pr_duration)
