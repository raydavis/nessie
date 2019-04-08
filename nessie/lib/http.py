"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research, and not-for-profit purposes, without fee and without a
signed licensing agreement, is hereby granted, provided that the above copyright
notice, this paragraph and the following two paragraphs appear in all copies,
modifications, and distributions.

Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.

IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
"AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.
"""

import logging
import urllib

from flask import current_app as app
from flask import Response
import requests
import simplejson as json


class ResponseExceptionWrapper:
    def __init__(self, exception, original_response=None):
        self.exception = exception
        self.raw_response = original_response

    def __bool__(self):
        return False

    def __repr__(self):
        return f'<ResponseExceptionWrapper exception={self.exception}, raw_response={self.raw_response}>'


def add_param_to_url(url, param):
    parsed_url = urllib.parse.urlparse(url)
    parsed_query = urllib.parse.parse_qsl(parsed_url.query)
    parsed_query.append(param)
    return urllib.parse.urlunparse(parsed_url._replace(query=urllib.parse.urlencode(parsed_query)))


def build_url(url, query=None):
    encoded_query = urllib.parse.urlencode(query, doseq=True, safe=',') if query else ''
    url_components = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse([
        url_components.scheme,
        url_components.netloc,
        urllib.parse.quote(url_components.path),
        '',
        encoded_query,
        '',
    ])


def get_next_page(response):
    if response.links and 'next' in response.links:
        return response.links['next'].get('url')
    else:
        return None


def request(url, headers={}, method='get', auth=None, auth_params=None, data=None, log_404s=True, **kwargs):
    """Exception and error catching wrapper for outgoing HTTP requests.

    :param url:
    :param headers:
    :return: The HTTP response from the external server, if the request was successful.
        Otherwise, a wrapper containing the exception and the original HTTP response, if
        one was returned.
        Borrowing the Requests convention, successful responses are truthy and failures are falsey.
    """
    if method not in ['get', 'post', 'put', 'delete']:
        raise ValueError(f'Unrecognized HTTP method "{method}"')
    app.logger.debug({'message': 'HTTP request', 'url': url, 'method': method, 'headers': sanitize_headers(headers)})
    response = None
    try:
        # By default, the urllib3 package used by Requests will log all request parameters at DEBUG level.
        # If authorization credentials were provided as params, keep them out of log files.
        if auth_params:
            urllib_logger = logging.getLogger('urllib3')
            saved_level = urllib_logger.level
            urllib_logger.setLevel(logging.INFO)
        http_method = getattr(requests, method)
        response = http_method(url, headers=headers, auth=auth, params=auth_params, json=data, **kwargs)
        if auth_params:
            urllib_logger.setLevel(saved_level)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        if not (hasattr(response, 'status_code') and response.status_code == 404 and not log_404s):
            app.logger.error(e)
            if hasattr(response, 'content'):
                app.logger.error(response.content)
        return ResponseExceptionWrapper(e, response)
    else:
        return response


def sanitize_headers(headers):
    """Suppress authorization token in logged headers."""
    if 'Authorization' in headers:
        # Canvas style.
        sanitized = headers.copy()
        sanitized['Authorization'] = 'Bearer <token>'
        return sanitized
    elif 'app_id' in headers:
        # Hub style.
        sanitized = headers.copy()
        sanitized['app_id'] = '<app_id>'
        sanitized['app_key'] = '<app_key>'
        return sanitized
    else:
        return headers


def tolerant_jsonify(obj, **kwargs):
    # In development the response can be shared with requesting code from any local origin.
    headers = {
        'Access-Control-Allow-Origin': 'http://localhost:8080',
        'Access-Control-Allow-Credentials': 'true',
    } if app.config['NESSIE_ENV'] == 'development' else {}
    content = json.dumps(obj, ignore_nan=True, separators=(',', ':'), **kwargs)
    return Response(content, mimetype='application/json', headers=headers)
