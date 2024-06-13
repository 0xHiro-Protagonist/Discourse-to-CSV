"""
Core API client module
"""
import csv
import requests
import logging
import time
from tracemalloc import start

import requests

from datetime import timedelta, datetime

from exceptions import (
    DiscourseError,
    DiscourseServerError,
    DiscourseClientError,
    DiscourseRateLimitedError,
)

log = logging.getLogger("client")

# HTTP verbs to be used as non string literals
DELETE = "DELETE"
GET = "GET"
POST = "POST"
PUT = "PUT"


class DiscourseClient(object):
    """Discourse API client"""

    def __init__(self, host, api_username, api_key, start_date, end_date, timeout=None):
        """
        Initialize the client

        Args:
            host: full domain name including scheme for the Discourse API
            api_username: username to connect with
            api_key: API key to connect with
            timeout: optional timeout for the request (in seconds)

        Returns:

        """
        self.host = host
        self.api_username = api_username
        self.api_key = api_key
        self.timeout = timeout
        self.start_date = start_date
        self.end_date = end_date

    def get_report(self):
        """
        Gets all of the reports for a day or a span of days and writes them to a CSV file
        """
        # Example structure of aggregated data
        aggregated_data = self.aggregate_data_by_date()

        # Write data to CSV
        self.write_to_csv(aggregated_data)

    def aggregate_data_by_date(self):
        """
        Fetch data from the API and aggregate it by date.
        """
        aggregated_data = {}

        # Fetch data for each metric
        topics_data = self.get_topics()
        posts_data = self.get_posts()
        solutions_data = self.get_solutions()
        registrations_data = self.get_new_users()
        user_views_data = self.get_user_views()
        anon_views_data = self.get_anon_views()
        crawlers_data = self.get_crawlers()
        forum_views_data = self.get_forum_views()
        users_engaged_data = self.get_users_engaged()
        no_response_data = self.get_no_response()
        likes_data = self.get_likes()

        # List of all metrics data for iteration
        all_metrics_data = [
            ('Topics', topics_data),
            ('Posts', posts_data),
            ('Solutions', solutions_data),
            ('Registrations', registrations_data),
            ('User views', user_views_data),
            ('Anon views', anon_views_data),
            ('Crawlers', crawlers_data),
            ('Forum Views', forum_views_data),
            ('Users engaged', users_engaged_data),
            ('No response', no_response_data),
            ('Likes', likes_data),
        ]

        # Initialize aggregated_data with dates as keys and metrics as nested keys
        for metric_name, metric_data in all_metrics_data:
            for entry in metric_data:
                date = entry['x']  # Assuming 'x' is the date
                count = entry['y']  # Assuming 'y' is the count

                if date not in aggregated_data:
                    aggregated_data[date] = {metric: 0 for metric, _ in all_metrics_data}

                aggregated_data[date][metric_name] += count

        return aggregated_data

    def write_to_csv(self, aggregated_data):
        """
        Writes the provided data to a CSV file.
        """
        filename = "discourse_report_{}_to_{}.csv".format(self.start_date, self.end_date)
        headers = ["Date", "Topics", "Posts", "Solutions", "Registrations", "User views", "Anon views", "Crawlers", "Forum Views", "Users engaged", "No response", "Likes"]

        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)  # Write the header row

            for date, metrics in aggregated_data.items():
                row = [date] + [metrics.get(header, '') for header in headers[1:]]  # Compile row data
                writer.writerow(row)

        print(f"Report written to {filename}")

    def get_topics(self):
        """
        Gets the number of topics for a given day or date range.
        """
        # Adjust the URL path and parameters as needed based on the actual API endpoint for topics
        return self._get("/admin/reports/bulk?reports%5Btopics%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Btopics%5D%5Bstart_date%5D={start}&reports%5Btopics%5D%5Bend_date%5D={end}".format(start=self.start_date, end=self.end_date))["reports"][0]["data"]
    def get_anon_views(self):
        """
        Gets the number of anon views for given day
        """
        return self._get("/admin/reports/bulk?reports%5Bconsolidated_page_views%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Bconsolidated_page_views%5D%5Bstart_date%5D={start}&reports%5Bconsolidated_page_views%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"][1]["data"]

    def get_crawlers(self):
        """
        Gets the number of crawlers views for given day
        """
        return self._get("/admin/reports/bulk?reports%5Bconsolidated_page_views%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Bconsolidated_page_views%5D%5Bstart_date%5D={start}&reports%5Bconsolidated_page_views%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"][2]["data"]
        
    def get_forum_views(self):
        """
        Gets the number of forum views for given day
        """
        anon = self.get_anon_views()
        crawlers = self.get_crawlers()
        users = self.get_user_views()
        res = []
        for count in range(0,len(anon)):
            temp = {}
            temp['x'] = anon[count]['x']
            temp['y'] = anon[count]['y'] + crawlers[count]['y'] + users[count]['y']
            res.append(temp)
        return res

    def get_likes(self):
        """
        Gets the number of likes for given day
        """
        return self._get("/admin/reports/bulk?reports%5Blikes%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Blikes%5D%5Bstart_date%5D={start}&reports%5Blikes%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"]
    
    def get_new_users(self):
        """
        Gets the number of new users for given day
        """
        return self._get("/admin/reports/bulk?reports%5Bsignups%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Bsignups%5D%5Bstart_date%5D={start}&reports%5Bsignups%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"]
    
    def get_no_response(self):
        """
        Gets the number of no responses for given day
        """
        no_response = self._get("/admin/reports/bulk?reports%5Btopics_with_no_response%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Btopics_with_no_response%5D%5Bstart_date%5D={start}&reports%5Btopics_with_no_response%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"]
        anon = self.get_anon_views()
        res = []
        for a in anon:
            temp = {}
            value = 0
            for n in no_response:
                if a['x'] == n['x']:
                    value = n['y']
            temp['x'] = a['x']
            temp['y'] = value
            res.append(temp)
        return res

    def get_posts(self):
        """
        Gets the number of posts for given day
        """
        return self._get("/admin/reports/bulk?reports%5Bposts%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Bposts%5D%5Bstart_date%5D={start}&reports%5Bposts%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"]

    def get_solutions(self):
        """
        Gets the number of solutions for given day
        """
        solutions = self._get("/admin/reports/bulk?reports%5Baccepted_solutions%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Baccepted_solutions%5D%5Bstart_date%5D={start}&reports%5Baccepted_solutions%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"]
        anon = self.get_anon_views()
        res = []
        for a in anon:
            temp = {}
            value = 0
            for s in solutions:
                if a['x'] == s['x']:
                    value = s['y']
            temp['x'] = a['x']
            temp['y'] = value
            res.append(temp)
        return res

    def get_user_views(self):
        """
        Gets the number of logged in views for given day
        """
        return self._get("/admin/reports/bulk?reports%5Bconsolidated_page_views%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Bconsolidated_page_views%5D%5Bstart_date%5D={start}&reports%5Bconsolidated_page_views%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"][0]["data"]

    def get_users_engaged(self):
        """
        Gets the number of users engaged for given day
        """
        return self._get("/admin/reports/bulk?reports%5Bdaily_engaged_users%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Bdaily_engaged_users%5D%5Bstart_date%5D={start}&reports%5Bdaily_engaged_users%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"]

    def get_time_to_response(self):
        """
        Gets the time to response for given day
        """
        return self._get("/admin/reports/bulk?reports%5Btime_to_first_response%5D%5Bfacets%5D%5B%5D=prev_period&reports%5Btime_to_first_response%5D%5Bstart_date%5D={start}&reports%5Btime_to_first_response%5D%5Bend_date%5D={end}".format(start=self.start_date,end=self.end_date))["reports"][0]["data"]

    def _get(self, path, override_request_kwargs=None, **kwargs):
        """

        Args:
            path:
            **kwargs:

        Returns:

        """
        return self._request(
            GET, path, params=kwargs, override_request_kwargs=override_request_kwargs
        )

    def _put(self, path, json=False, override_request_kwargs=None, **kwargs):
        """

        Args:
            path:
            **kwargs:

        Returns:

        """
        if not json:
            return self._request(
                PUT, path, data=kwargs, override_request_kwargs=override_request_kwargs
            )

        else:
            return self._request(
                PUT, path, json=kwargs, override_request_kwargs=override_request_kwargs
            )

    def _post(
        self, path, files=None, json=False, override_request_kwargs=None, **kwargs
    ):
        """

        Args:
            path:
            **kwargs:

        Returns:

        """
        if not json:
            return self._request(
                POST,
                path,
                files=files,
                data=kwargs,
                override_request_kwargs=override_request_kwargs,
            )

        else:
            return self._request(
                POST,
                path,
                files=files,
                json=kwargs,
                override_request_kwargs=override_request_kwargs,
            )

    def _delete(self, path, override_request_kwargs=None, **kwargs):
        """

        Args:
            path:
            **kwargs:

        Returns:

        """
        return self._request(
            DELETE, path, params=kwargs, override_request_kwargs=override_request_kwargs
        )

    def _request(
        self,
        verb,
        path,
        params=None,
        files=None,
        data=None,
        json=None,
        override_request_kwargs=None,
    ):
        """
        Executes HTTP request to API and handles response

        Args:
            verb: HTTP verb as string: GET, DELETE, PUT, POST
            path: the path on the Discourse API
            params: dictionary of parameters to include to the API
            override_request_kwargs: dictionary of requests.request
                    keyword arguments to override defaults

        Returns:
            dictionary of response body data or None

        """
        override_request_kwargs = override_request_kwargs or {}

        url = self.host + path

        headers = {
            "Accept": "application/json; charset=utf-8",
            "Api-Key": self.api_key,
            "Api-Username": self.api_username,
        }

        retry_count = 4
        retry_backoff = 1

        while retry_count > 0:
            request_kwargs = dict(
                allow_redirects=False,
                params=params,
                files=files,
                data=data,
                json=json,
                headers=headers,
                timeout=self.timeout,
            )

            request_kwargs.update(override_request_kwargs)
            print("Requesting URL:", url)
            response = requests.request(verb, url, **request_kwargs)

            log.debug("response %s: %s", response.status_code, repr(response.text))
            if response.ok:
                break
            if not response.ok:
                try:
                    msg = u",".join(response.json()["errors"])
                except (ValueError, TypeError, KeyError):
                    if response.reason:
                        msg = response.reason
                    else:
                        msg = u"{0}: {1}".format(response.status_code, response.text)

                if 400 <= response.status_code < 500:
                    if 429 == response.status_code:
                        rj = response.json()
                        wait_delay = (
                            retry_backoff + rj["extras"]["wait_seconds"]
                        )  

                        if retry_count > 1:
                            time.sleep(wait_delay)
                        retry_count -= 1
                        log.info(
                            "We have been rate limited and waited {0} seconds ({1} retries left)".format(
                                wait_delay, retry_count
                            )
                        )
                        log.debug("API returned {0}".format(rj))
                        continue
                    else:
                        raise DiscourseClientError(msg, response=response)

                raise DiscourseServerError(msg, response=response)

        if retry_count == 0:
            raise DiscourseRateLimitedError(
                "Number of rate limit retries exceeded. Increase retry_backoff or retry_count",
                response=response,
            )

        if response.status_code == 302:
            raise DiscourseError(
                "Unexpected Redirect, invalid api key or host?", response=response
            )

        json_content = "application/json; charset=utf-8"
        content_type = response.headers["content-type"]
        if content_type != json_content:
            if not response.content.strip():
                return None

            raise DiscourseError(
                'Invalid Response, expecting "{0}" got "{1}"'.format(
                    json_content, content_type
                ),
                response=response,
            )

        try:
            decoded = response.json()
        except ValueError:
            raise DiscourseError("failed to decode response", response=response)

        if "errors" in decoded and len(decoded["errors"]) > 0:
            message = decoded.get("message")
            if not message:
                message = u",".join(decoded["errors"])
            raise DiscourseError(message, response=response)

        return decoded
