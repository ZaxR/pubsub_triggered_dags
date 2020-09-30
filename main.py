"""Google Cloud Function code that will trigger an Airflow DAG

with helper functions to support various types of Google service authentications
and a naive approach to enforce exactly-once triggering.
"""
import logging
import os
from contextlib import contextmanager

import google.auth
import google.auth.transport.requests
import requests
import six.moves.urllib.parse
from flask import Response
from google.cloud import secretmanager
from google.oauth2 import id_token
from sqlalchemy import Column, create_engine, DateTime, engine, func, Integer, Text
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'

# Note: you may want to access different projects for different resources,
# but this example uses the same project for each service.
# feel free to pass different env vars for each
# via the `gcloud functions deploy` --update-env-vars flag
GOOGLE_PROJECT_NAME = os.environ.get("GOOGLE_PROJECT_NAME")
GCS_PROJECT_NAME = os.environ.get("GOOGLE_PROJECT_NAME")
GOOGLE_LOCATION = "us-central1"
COMPOSER_ENVIRONMENT_NAME = os.environ.get('COMPOSER_ENVIRONMENT_NAME')

IAP_TIMEOUT = int(os.environ.get("IAP_TIMEOUT", 90))
DAG_NAME = os.environ.get("DAG_NAME")


def get_secret_manager_client(credentials=None):
    credentials = credentials or google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])[0]
    return secretmanager.SecretManagerServiceClient(credentials=credentials)


def get_password(key: str, client=None) -> str:
    """Get a Google Secret password."""
    client = client or get_secret_manager_client()
    name = client.secret_version_path(GCS_PROJECT_NAME, key, "latest")
    response = client.access_secret_version(name)
    return response.payload.data.decode('UTF-8')


def get_authed_google_session():
    """ Authenticate with Google Cloud.

    See: https://cloud.google.com/docs/authentication/getting-started

    """
    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    authed_session = google.auth.transport.requests.AuthorizedSession(credentials)

    return authed_session


def get_airflow_uri() -> str:
    environment_url = (f'https://composer.googleapis.com/v1beta1/'
                       f'projects/{GOOGLE_PROJECT_NAME}/'
                       f'locations/{GOOGLE_LOCATION}/'
                       f'environments/{COMPOSER_ENVIRONMENT_NAME}')

    authed_session = get_authed_google_session()
    composer_response = authed_session.request('GET', environment_url)
    environment_data = composer_response.json()
    airflow_uri = environment_data['config']['airflowUri']

    return airflow_uri


def get_airflow_webserver_id() -> str:
    """ Get the Google Composer webserver ID.

    Found in your webserver's URL: {webserver_id}.appspot.com

    """
    airflow_uri = get_airflow_uri()
    webserver_id = airflow_uri[airflow_uri.find("https://") + len("https://"): airflow_uri.rfind(".appspot.com")]

    return webserver_id


def get_client_id() -> str:
    """

    Alternatively:
        `curl -v <airflow-url>`, e.g. https://bc5c0e43e23571a62-tp.appspot.com/`
        And the client_id will be printed in the URL
        after "https://accounts.google.com/o/oauth2/v2/auth?client_id="

    """
    airflow_uri = get_airflow_uri()

    # The Composer environment response does not include the IAP client ID.
    # Make a second, unauthenticated HTTP request to the web server to get the redirect URI.
    redirect_response = requests.get(airflow_uri, allow_redirects=False)
    redirect_location = redirect_response.headers['location']

    # Extract the client_id query parameter from the redirect.
    parsed = six.moves.urllib.parse.urlparse(redirect_location)
    query_string = six.moves.urllib.parse.parse_qs(parsed.query)

    return query_string['client_id'][0]


def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.

    Code copied from https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/iap/make_iap_request.py

    Args:
        url: The Identity-Aware Proxy-protected URL to fetch.
        client_id: The client ID used by Identity-Aware Proxy.
        method: The request method to use
                ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
        **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.

    Returns:
        The page body, or raises an exception if the page couldn't be retrieved.

    """
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service account.
    google_open_id_connect_token = id_token.fetch_id_token(
        google.auth.transport.requests.Request(), client_id
    )

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    logger.info("Attempting iap auth")
    resp = requests.request(
        method,
        url,
        headers={'Authorization': f'Bearer {google_open_id_connect_token}'},
        **kwargs
    )
    logger.info("Past auth")
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text


Base = declarative_base()


class PubSubHistory(Base):
    """Table to keep track of PubSub messages we've already seen."""
    __tablename__ = "pubsub_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    message_id = Column(Text, nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


def get_engine():
    DB_USERNAME = get_password("DB_USERNAME")
    DB_PASSWORD = get_password("DB_PASSWORD")
    DB_NAME = get_password("DB_NAME")
    # Proxy name pattern: "project:location:db"
    CLOUD_SQL_PROXY_INSTANCE_CONNECTION_NAME = get_password("CLOUD_SQL_PROXY_INSTANCE_CONNECTION_NAME")

    if CLOUD_SQL_PROXY_INSTANCE_CONNECTION_NAME:
        host = f"/cloudsql/{CLOUD_SQL_PROXY_INSTANCE_CONNECTION_NAME}"
    else:
        host = "docker-compose-test-db-service-name"

    url = engine.url.URL(
        drivername="postgres+psycopg2",
        username=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=f"/cloudsql/{CLOUD_SQL_PROXY_INSTANCE_CONNECTION_NAME}"
    )

    eng = create_engine(
        url,
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800
    )

    Base.metadata.create_all(bind=eng)

    return eng


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    session = session_factory()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def message_id_exists(message_id: str) -> bool:
    """
    See: https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    """
    with session_scope() as session:
        message_id_count = (
            session
            .query(PubSubHistory)
            .filter(PubSubHistory.message_id == message_id)
            .count()
        )

    return True if message_id_count > 0 else False


def trigger_dag(request, context=None):
    """HTTP Cloud function that makes a POST request to the Composer DAG Trigger API.

    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.

    For more info, refer to
    https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-python

    To call this function from a Python script, omit the ``context`` argument
    and pass in a non-null value for the ``data`` argument.

    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>

    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.

    """
    if os.environ.get('ENABLED').lower() == "false":
        return Response("Cloud function is disabled, because it's a tea pot.", status=418)

    request_json = request.get_json(silent=True)

    if request_json and 'message' in request_json:
        message_id = request_json['message']['messageId']
        logger.info(f"Received message with messageId {message_id}")
        attributes = request_json['message'].get('attributes', {})
        # TODO: log record of trigger to postgres db
    else:
        return Response("Error - Malformed Event Notification", status=500)

    # If the PubSubHistory table is locked, queries will wait for the table to be unlocked
    # If messageId already exists in table, return 200
    if message_id_exists(message_id):
        return Response(f"The message for messageId {message_id} has already been processed.", status=200)

    # Otherwise, try to obtain a lock and trigger the dag
    # return 200 if a new lock was introduced
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    session = session_factory()
    # session.begin_nested()
    try:
        # See https://cloud.google.com/blog/products/serverless/cloud-functions-pro-tips-building-idempotent-functions
        # for the reason for this lock.
        # See https://stackoverflow.com/a/37396201/7619676
        # for the basis for the lock code
        session.execute('LOCK TABLE pubsub_history IN ACCESS EXCLUSIVE MODE NOWAIT;')
    except OperationalError as e:
        # lock_not_available.
        # See https://www.postgresql.org/docs/12/errcodes-appendix.html
        if e.orig.pgcode == '55P03':
            session.rollback()
            return Response(
                f"The message for messageId {message_id} has already been processed.",
                status=200
            )
        else:
            raise e

    new_message_id_row = PubSubHistory(message_id=message_id)
    session.add(new_message_id_row)
    session.commit()
    session.close()

    logger.info("Attributes: " + str(attributes))
    if (
        attributes.get("data_type") == "sales_data"
        and attributes.get("process") == "cleaning"
        and attributes.get("status") == "completed"
    ):
        client_id = get_client_id()  # E.g. '573384987581-8au6w57k7ynq3abrr5ffhdkhs435bor1.apps.googleusercontent.com'
        webserver_id = get_airflow_webserver_id()  # E.g. 'bc5c0e43e23571a62-tp'
        # The name of the DAG you wish to trigger
        webserver_url = (
            f"https://{webserver_id}.appspot.com/api/experimental/dags/{DAG_NAME}/dag_runs"
        )
        # Make a POST request to IAP which then Triggers the DAG
        # "conf" values are optional DAG parameters.
        # Example: https://github.com/GoogleCloudPlatform/professional-services/blob/master/examples/cloud-composer-examples/composer_http_post_example/dag_trigger.py#L75
        logger.info("Running make_iap_request...")
        make_iap_request(
            webserver_url,
            client_id,
            method='POST',
            timeout=IAP_TIMEOUT,
            json={"conf": message_id}
        )
