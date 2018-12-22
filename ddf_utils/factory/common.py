# -*- coding: utf-8 -*-

import attr
from abc import abstractmethod, ABC

import requests as req
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# from https://www.peterbe.com/plog/best-practice-with-retries-with-requests
def requests_retry_session(
    retries=5,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or req.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


@attr.s
class DataFactory(ABC):
    metadata = attr.ib(init=False, default=None)

    @abstractmethod
    def load_metadata(self, *args, **kwargs):
        ...

    @abstractmethod
    def has_newer_source(self, *args, **kwargs):
        ...

    @abstractmethod
    def bulk_download(self, *args, **kwargs):
        ...
