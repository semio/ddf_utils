# -*- coding: utf-8 -*-

import os.path as osp
import attr
from abc import abstractmethod, ABC

import requests as req
from time import sleep
from functools import wraps
from requests import Request
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import ChunkedEncodingError
from tqdm import tqdm


# from https://www.peterbe.com/plog/best-practice-with-retries-with-requests
def requests_retry_session(
        retries=5,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
):
    session = session or req.Session()
    max_retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=max_retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def retry(times=5, backoff=0.5, exceptions=(Exception)):
    """general wrapper to retry things"""
    def wrapper(func):
        @wraps(func)
        def newfunc(*args, **kwargs):
            mtimes = times
            ttimes = times
            while ttimes > 0:
                try:
                    res = func(*args, **kwargs)
                except KeyboardInterrupt:
                    raise
                except exceptions as e:
                    ttimes = ttimes - 1
                    if ttimes == 0:
                        raise
                    sleep(backoff * (mtimes - ttimes))
                else:
                    break
            return res
        return newfunc
    return wrapper


def download(url, out_file, session=None, resume=True, method="GET", post_data=None,
             retry_times=5, backoff=0.5, progress_bar=True, timeout=30):
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
    method : `str`
        could be "GET" or "POST". When posting you can pass a dictionary to `post_data`
    post_data : dict
    times : int
    backoff : float
    progress_bar : bool
        whether to display a progress bar
    timeout : int
        maximum time to wait for connect/read server responses. (Note: not the time limit for total response)
    """
    # start session
    if not session:
        session = req.Session()

    # prepare the request
    # to ensure requests don't get modify for retries, we use a function to create them.
    def prepare_request():
        if method == 'GET':
            basereq = Request(method='GET', url=url)
        elif method == 'POST':
            basereq = Request(method='POST', url=url, data=post_data)
        else:
            raise ValueError("method {} not supported".format(method))

        request = session.prepare_request(basereq)
        return request

    @retry(times=retry_times, backoff=backoff, exceptions=(Exception, ChunkedEncodingError))
    def get_response(first_byte=None, file_size=None):
        request = prepare_request()
        if first_byte and file_size:
            request.headers['Range'] = f'bytes={first_byte}-{file_size}'
        response = session.send(request, stream=True, timeout=timeout)
        response.raise_for_status()
        return response

    # main function body
    print(f"begin downloading {out_file}...")
    if resume:
        mode = 'ab'
    else:
        mode = 'wb'

    response = get_response()
    file_size = int(response.headers['Content-Length'])

    if osp.exists(out_file) and resume:
        first_byte = osp.getsize(out_file)
        if first_byte == file_size:
            print("download was completed")
            return
        elif first_byte < file_size:
            print(f'resumming {out_file}...')
            response = get_response(first_byte, file_size)
        else:
            print(f'file broken: {out_file}, re-download entire file')
            # ignore resume mode, download it again
            mode = 'wb'
            first_byte = 0
    else:
        first_byte = 0

    if progress_bar:
        pbar = tqdm(total=file_size, initial=first_byte, unit='B', unit_scale=True)

    with open(out_file, mode) as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
                if progress_bar:
                    pbar.update(1024)
        f.close()
    if progress_bar:
        pbar.close()
    if osp.getsize(out_file) != file_size:
        raise ValueError(f"file size doesn't match content length from server: {out_file}")
    return


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
