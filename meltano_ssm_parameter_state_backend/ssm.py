"""StateStoreManager for AWS SSM backend."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from functools import cached_property
from urllib.parse import urlparse

from meltano.core.job_state import JobState
from meltano.core.state_store.base import StateStoreManager
from meltano.core.state_store.filesystem import (
    InvalidStateBackendConfigurationException,
)

BOTO_INSTALLED = True

try:
    import boto3
except ImportError:
    BOTO_INSTALLED = False

logger = logging.getLogger(__name__)


class MissingBoto3Error(Exception):
    """Raised when boto3 is required but not installed."""

    def __init__(self):
        """Initialize a MissingBoto3Error."""
        super().__init__(
            "boto3 required but not installed. Install meltano[s3] to use S3 as a state backend.",  # noqa: E501
        )


@contextmanager
def requires_boto3():
    """Raise MissingBoto3Error if boto3 is required but missing in context.

    Raises:
        MissingBoto3Error: if boto3 is not installed.

    Yields:
        None
    """
    if not BOTO_INSTALLED:
        raise MissingBoto3Error
    yield


class SSMStateStoreManager(StateStoreManager):
    """State backend for SSM."""

    label: str = "AWS SSM"

    def __init__(
        self,
        uri : str,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        endpoint_url: str | None = None,
        **kwargs,
    ):
        """Initialize the BaseFilesystemStateStoreManager.

        Args:
            aws_access_key_id: AWS access key ID used to authenticate
            aws_secret_access_key: AWS secret access key used to authenticate
            prefix: the prefix to store state at
            endpoint_url: the endpoint url for SSM
            kwargs: additional keyword args to pass to parent
        """
        super().__init__(**kwargs)
        self.uri = uri
        self.parsed = urlparse(self.uri)
        self.prefix = f"/{self.parsed.netloc}{self.parsed.path}"
        if self.prefix.endswith("/"):
            self.prefix = self.prefix[:-1] 
        self.aws_access_key_id = aws_access_key_id  # or self.parsed.username
        self.aws_secret_access_key = aws_secret_access_key  # or self.parsed.password
        self.endpoint_url = endpoint_url

    def _convert_state_id_to_ssm_path(self,state_id):
        return f"{self.prefix}/{state_id.replace(':','/')}"

    def _convert_ssm_path_to_state_id(self,ssm_path):
        return ssm_path.replace(self.prefix + "/", "", 1).replace("/", ":")

    @cached_property
    def client(self):
        """Get an authenticated boto3.Client.

        Returns:
            A boto3.Client.

        Raises:
            InvalidStateBackendConfigurationException: when configured AWS
                settings are invalid.
        """
        with requires_boto3():
            if self.aws_secret_access_key and self.aws_access_key_id:
                session = boto3.Session(
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                )
                return session.client("ssm", endpoint_url=self.endpoint_url)
            elif self.aws_secret_access_key:
                raise InvalidStateBackendConfigurationException(
                    "AWS secret access key configured, but not AWS access key ID.",
                )
            elif self.aws_access_key_id:
                raise InvalidStateBackendConfigurationException(
                    "AWS access key ID configured, but no AWS secret access key.",
                )
            session = boto3.Session()
            return session.client("ssm")

    def set(self, state: JobState):
        """Set state for the given state_id.

        Args:
            state: the state to set

        Raises:
            Exception: if error not indicating file is not found is thrown
        """
        logger.info(f"Writing state to {self.label}")
        logger.info(f"{state.state_id=}")
        logger.info(f"{self.prefix=}")
        ssm_path = self._convert_state_id_to_ssm_path(state.state_id)
        logger.info(f"{ssm_path=}")
        response = self.client.put_parameter(
            Name=ssm_path, Value=state.json(), Type="String"
        )

    def get(self, state_id):
        """Get the job state for the given state_id.

        Args:
            state_id: the name of the job to get state for

        Returns:
            The current state for the given job
        """
        ssm_path = self._convert_state_id_to_ssm_path(state_id)
        logger.info(f"{ssm_path=}")
        response = self.client.get_parameter(Name=ssm_path)
        parameter = response.get("Parameter")
        parameter_value = parameter.get("Value")
        return JobState.from_json(state_id, parameter_value)

    def clear(self, state_id):
        """Clear state for the given state_id.

        Args:
            state_id: the state_id to clear state for
        """
        # self.client.delete_parameter
        ssm_path = self._convert_state_id_to_ssm_path(state_id)
        logger.info(f"{ssm_path=}")
        response = self.client.delete_parameter(Name=ssm_path)

    def get_state_ids(self, pattern: str | None = None):
        """Get all state_ids available in this state store manager.

        Args:
            pattern: glob-style pattern to filter by

        Returns:
            Generator yielding names of available jobs
        """
        # get-parameters-by-path - and apply the 'pattern'
        logger.info(f"Looking for parameters at {self.prefix}")
        ssm_paginator = self.client.get_paginator("get_parameters_by_path")
        parameters = []
        for page in ssm_paginator.paginate(Path=self.prefix,Recursive=True):
            parameters += [self._convert_ssm_path_to_state_id(parameter.get('Name')) for parameter in page["Parameters"]]
        logger.info(f"{parameters=}")
        return parameters

    def acquire_lock(self, state_id):
        """Acquire a naive lock for the given job's state.

        For DBStateStoreManager, the db manages transactions.
        This does nothing.

        Args:
            state_id: the state_id to lock
        """
        ...  # noqa: WPS428

    def release_lock(self, state_id):
        """Release the lock for the given job's state.

        For DBStateStoreManager, the db manages transactions.
        This does nothing.

        Args:
            state_id: the state_id to unlock
        """
        ...  # noqa: WPS428