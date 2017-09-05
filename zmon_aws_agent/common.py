import time
import logging

import opentracing
import opentracing.ext.tags as tags
from urllib.parse import urlparse

from botocore.exceptions import ClientError

from zmon_aws_agent import __version__

MAX_RETRIES = 10
TIME_OUT = 0.5

logger = logging.getLogger(__name__)


def get_user_agent():
    return 'zmon-aws-agent/{}'.format(__version__)


def get_sleep_duration(retries):
    return 2 ** retries * TIME_OUT


def call_and_retry(fn, *args, **kwargs):
    """Call `fn` and retry in case of API Throttling exception."""
    count = 0

    while True:
        try:
            return fn(*args, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == 'Throttling' or \
               'RequestLimitExceeded' in str(e):
                if count < MAX_RETRIES:
                    logger.info('Throttling AWS API requests...')
                    time.sleep(get_sleep_duration(count))
                    count += 1
                    continue
            raise


def trace_span(name):
    return opentracing.tracer.start_span(operation_name=name)


def trace_http(name, parent_span, request):
    outbound_span = opentracing.tracer.start_span(
        operation_name=name,
        child_of=parent_span
    )

    u = urlparse(request.url)
    outbound_span.set_tag('http.url', request.url)
    service_name = request.url
    if ':' in u.netloc:
        host, port = u.netloc.split(':', 1)
    else:
        host = u.netloc
        port = None
    if service_name:
        outbound_span.set_tag(tags.PEER_SERVICE, service_name)
    if host:
        outbound_span.set_tag(tags.PEER_HOST_IPV4, host)
    if port:
        outbound_span.set_tag(tags.PEER_PORT, port)

    http_header_carrier = {}
    opentracing.tracer.inject(outbound_span,
                              opentracing.Format.HTTP_HEADERS,
                              http_header_carrier)

    request.headers.update(http_header_carrier)

    return outbound_span
