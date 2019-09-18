# -*- coding: utf-8 -*-

import os.path as osp
import attr
from abc import abstractmethod, ABC

import requests as req
from time import sleep
from functools import wraps
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm


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


def retry(times=5, backoff=0.5):
    """general wrapper to retry things"""
    def wrapper(func):
        @wraps(func)
        def newfunc(*args, **kwargs):
            mtimes = times
            ttimes = times
            while ttimes > 0:
                try:
                    func(*args, **kwargs)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    ttimes = ttimes - 1
                    if ttimes == 0:
                        raise
                    sleep(backoff * (mtimes - ttimes))
                else:
                    break
        return newfunc
    return wrapper


def download(url, out_file, session=None, resume=True, retry_times=5, backoff=0.5, progress_bar=True):
    """Download a url, and optionally try to resume it.

    Parameters
    ==========

    url : `str`
        URL to be downloaded
    out_file : `filepath`
        output file path
    session : requests session object
        Please note that if you want to use `requests_retry_session`, you must not
        use resume=True
    resume : bool
        whether to resume the download
    times : int
    backoff : float
    progress_bar : bool
        whether to display a progress bar
    """
    @retry(times=retry_times, backoff=backoff)
    def run(url_, out_file_, session_, resume_):
        if osp.exists(out_file_) and resume_:
            first_byte = osp.getsize(out_file_)
        else:
            first_byte = 0
        if not session_:
            session_ = req.Session()

        response = session_.get(url_, stream=True)
        response.raise_for_status()
        file_size = int(response.headers['content-length'])

        if first_byte >= file_size:
            print("download was completed")
            return

        if first_byte > 0:
            print('resumming...')
            header = {"Range": f'bytes={first_byte}-{file_size}'}
            response = session_.get(url_, stream=True, headers=header)
            response.raise_for_status()

        if progress_bar:
            pbar = tqdm(total=file_size, initial=first_byte, unit='B', unit_scale=True)
        with open(out_file_, 'ab') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    if progress_bar:
                        pbar.update(1024)
            f.close()
        if progress_bar:
            pbar.close()
        if osp.getsize(out_file_) < file_size:
            raise ValueError
        return

    run(url, out_file, session, resume)


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
