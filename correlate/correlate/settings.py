"""
Django settings for correlate project.

Generated by 'django-admin startproject' using Django 5.0.1.

For more information on this file, see
https://docs.djangoproject.com/en/5.0/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/5.0/ref/settings/
"""

from pathlib import Path
import os
import dj_database_url
import environ

import sentry_sdk
import warnings


# Ignore pandas warnings
warnings.filterwarnings("ignore")


env = environ.Env()
environ.Env.read_env()

LOCAL_DEV = env.bool("LOCAL_DEV", default=False)  # type:ignore

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_ROOT = os.path.join(BASE_DIR, "staticfiles")


sentry_sdk.init(
    dsn="https://44990de3e2b5e22e22533ee906f4a07a@o4506736104112128.ingest.sentry.io/4506736106930176",
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
    environment="development" if LOCAL_DEV else "production",
)


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "django-insecure-1gsgldh1jz-od#lbdmhq#0w*8som8dpeggp747dj^rihkg1k7l"

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env.bool("DEBUG", default=False)  # type:ignore

ALLOWED_HOSTS = [
    "correlate-backend-e2905dab5cac.herokuapp.com",
    "localhost",
    "api.correlatefinance.com",
]


# Application definition

INSTALLED_APPS = [
    "whitenoise.runserver_nostatic",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "corsheaders",
    "rest_framework",
    "rest_framework.authtoken",
    "django_extensions",
    "ddtrace.contrib.django",
    "users",
    "datasets",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

CSRF_TRUSTED_ORIGINS = [
    "https://api.correlatefinance.com",
]

ROOT_URLCONF = "correlate.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": ["templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "correlate.wsgi.application"


# Database
# https://docs.djangoproject.com/en/5.0/ref/settings/#databases
DATABASES = {"default": dj_database_url.config(default=env("DATABASE_URL"))}


# Password validation
# https://docs.djangoproject.com/en/5.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# Internationalization
# https://docs.djangoproject.com/en/5.0/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.0/howto/static-files/

STATIC_ROOT = BASE_DIR / "staticfiles"
STATIC_URL = "static/"

STORAGES = {
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}

# Default primary key field type
# https://docs.djangoproject.com/en/5.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

AUTH_USER_MODEL = "users.User"

CORS_ALLOW_ALL_ORIGINS = True
CORS_ALLOW_CREDENTIALS = True

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": ("users.auth.TokenAuthSupportCookie",),
}


# Email
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
EMAIL_HOST = "smtp.purelymail.com"
EMAIL_PORT = 587
EMAIL_HOST_USER = env.str("EMAIL_HOST_USER", default="")  # type:ignore
EMAIL_HOST_PASSWORD = env.str("EMAIL_HOST_PASSWORD", default="")  # type:ignore
EMAIL_TIMEOUT = 15

# API KEYS
FRED_API_KEY = env.str("FRED_API_KEY", default="")  # type:ignore
EIA_API_KEY = env.str("EIA_API_KEY", default="")  # type:ignore
OPENAI_API_KEY = env.str("OPENAI_API_KEY", default="")  # type:ignore
DCF_API_KEY = env.str("DCF_API_KEY", default="")  # type:ignore
# Rust Engine
RUST_ENGINE_URL = env.str(
    "RUST_ENGINE_URL",
    default="https://api2.correlatefinance.com",  # type:ignore
)

# Celery
CELERY_BROKER_URL = env.str("CLOUDAMQP_URL", default="amqp://localhost")  # type:ignore
