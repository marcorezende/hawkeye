"""
For more configuration options, see:
- https://superset.apache.org/docs/configuration/configuring-superset
"""

import os
from flask_caching.backends.rediscache import RedisCache

SECRET_KEY = os.getenv("SECRET_KEY")
MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY", "")

FEATURE_FLAGS = {
    "THUMBNAILS": True,
    "THUMBNAILS_SQLA_LISTENERS": True,
}

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": "redis",
    "CACHE_REDIS_PORT": 6379,
    "CACHE_REDIS_DB": 1,
    "CACHE_REDIS_URL": f"redis://:{os.getenv('REDIS_PASSWORD')}@{os.getenv('REDIS_HOST')}:6379/0",
}
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SupersetMetastoreCache",
    "CACHE_KEY_PREFIX": "superset_results",  # make sure this string is unique to avoid collisions
    "CACHE_DEFAULT_TIMEOUT": 86400,  # 60 seconds * 60 minutes * 24 hours
}
THUMBNAIL_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 86400,
    'CACHE_KEY_PREFIX': 'thumbnail_',
    'CACHE_REDIS_URL': f"redis://:{os.getenv('REDIS_PASSWORD')}@{os.getenv('REDIS_HOST')}:6379/1"
}


class CeleryConfig(object):
    BROKER_URL = f"redis://:{os.getenv('REDIS_PASSWORD')}@{os.getenv('REDIS_HOST')}:6379/0"
    CELERY_IMPORTS = (
        'superset.sql_lab',
        'superset.tasks',
        'superset.tasks.thumbnails'
    )
    CELERY_RESULT_BACKEND = f"redis://:{os.getenv('REDIS_PASSWORD')}@{os.getenv('REDIS_HOST')}:6379/1"
    CELERYD_LOG_LEVEL = 'DEBUG'
    CELERYD_PREFETCH_MULTIPLIER = 10
    CELERY_ACKS_LATE = True
    CELERY_ANNOTATIONS = {
        'sql_lab.get_sql_results': {
            'rate_limit': '100/s',
        },
        'email_reports.send': {
            'rate_limit': '1/s',
            'time_limit': 120,
            'soft_time_limit': 150,
            'ignore_result': True,
        },
    }


CELERY_CONFIG = CeleryConfig
SESSION_COOKIE_SAMESITE = "None"  # One of [None, 'Lax', 'Strict']
SESSION_COOKIE_HTTPONLY = False
WTF_CSRF_ENABLED = False
RESULTS_BACKEND = RedisCache(
    host=os.getenv('REDIS_HOST'),
    port=6379,
    password=os.getenv('REDIS_PASSWORD'),
    key_prefix='superset_results'
)

WEBDRIVER_TYPE = "firefox"
WEBDRIVER_BASEURL = "http://superset:8088"  # or whatever URL your superset instance is running on, the webdriver will use this URL to connect to superset in order to take a screenshot
THUMBNAIL_SELENIUM_USER = "admin"  # any user with permissions to view all charts is sufficient

FILTER_STATE_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_filter_"}
EXPLORE_FORM_DATA_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_explore_form_"}

SQLALCHEMY_TRACK_MODIFICATIONS = True
SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:5432/{os.getenv('POSTGRES_DB')}"

# Uncomment if you want to load example data (using "superset load_examples") at the
# same location as your metadata postgresql instance. Otherwise, the default sqlite
# will be used, which will not persist in volume when restarting superset by default.
# SQLALCHEMY_EXAMPLES_URI = SQLALCHEMY_DATABASE_URI
