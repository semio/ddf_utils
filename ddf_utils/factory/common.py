# -*- coding: utf-8 -*-

import sys
import signal
import os.path as osp
import attr
from abc import abstractmethod, ABC
import pycurl
import certifi
import requests as req
from time import sleep
from functools import wraps
from urllib.parse import urlencode
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


def handle_ctrl_c(signal, frame):
    print("Got ctrl+c, going down!")
    sys.exit(0)


def retry(times=5, backoff=0.5, exceptions=(Exception)):
    """general wrapper to retry things"""
    def wrapper(func):
        @wraps(func)
        def newfunc(*args, **kwargs):
            # set ctrl-c to just break the loop
            signal.signal(signal.SIGINT, handle_ctrl_c)
            mtimes = times
            ttimes = times
            while ttimes > 0:
                try:
                    res = func(*args, **kwargs)
                except exceptions as e:
                    print(e)
                    ttimes = ttimes - 1
                    if ttimes == 0:
                        raise
                    sleep(backoff * (mtimes - ttimes))
                    print("retrying...")
                else:
                    break
            # reset ctrl-c
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            return res
        return newfunc
    return wrapper


class DownloadAcceptor():
    def __init__(self, handle, path, resume):
        self.handle = handle
        self.path = path
        self.resume = resume
        self.file_handle = None

    def open_file(self):
        if self.file_handle is not None:
            return self.file_handle
        if osp.exists(self.path) and self.resume:
            self.file_handle = open(self.path, 'ab')
        else:
            # print('file not exist')
            self.file_handle = open(self.path, 'wb')
        return self.file_handle

    def write(self, b):
        f = self.open_file()
        f.write(b)

    def close(self):
        self.file_handle.close()
        self.file_handle = None


def download(url, out_file, resume=True, method=None, post_data=None,
             retry_times=5, backoff=0.5, progress_bar=True, timeout=30, **kwargs):
    """
    Download a url to local file.
    This downloader use libcurl under the hood.

    Parameters
    ==========

    url : `str`
        URL to be downloaded
    out_file : `filepath`
        output file path
    resume : bool
        whether to resume the download
    method : `str`
        do not use this parameter. It's here because download() used to have this parameter.
        It will be removed eventually.
    post_data : dict
    times : int
    backoff : float
    progress_bar : bool
        whether to display a progress bar
    timeout : int
        maximum time to wait for connect/read server responses.

    Note
    ====

    when setting resume = True, it's user's responsibility to ensure the file
    on disk is the same version as the file on server.
    """
    def prepare_curl():
        # TODO: add more configurations from kwargs
        c = pycurl.Curl()
        c.setopt(c.FOLLOWLOCATION, True)
        c.setopt(c.URL, url)
        c.setopt(c.TIMEOUT, timeout)
        c.setopt(c.CAINFO, certifi.where())  # For HTTPS
        c.setopt(c.USERAGENT, "ddf_utils/1.0")
        c.setopt(c.COOKIEFILE, "")
        # c.setopt(c.VERBOSE, True)
        if progress_bar:
            c.setopt(c.NOPROGRESS, False)
        if post_data:
            c.setopt(c.POSTFIELDS, urlencode(post_data))
        return c

    @retry(times=retry_times, backoff=backoff, exceptions=(Exception))
    def run(resume):
        c = prepare_curl()
        acceptor = DownloadAcceptor(c, out_file, resume)
        if resume and osp.exists(out_file):
            first_byte = osp.getsize(out_file)
            c.setopt(c.RANGE, f"{first_byte}-")
        c.setopt(c.WRITEFUNCTION, acceptor.write)
        c.perform()
        if c.getinfo(c.HTTP_CODE) == 416:
            print("the http status code is 416, possibly the download was completed.")
            print("if you believe it's not completed, please remove the file and try again.")
        c.close()
        return

    run(resume)



def download_2(url, out_file, session=None, resume=True, method="GET", post_data=None,
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
    def prepare_request(request_method):
        if request_method in ('GET', 'HEAD'):
            basereq = Request(method=request_method, url=url)
        elif request_method == 'POST':
            basereq = Request(method='POST', url=url, data=post_data)
        else:
            raise ValueError("method {} not supported".format(method))

        request = session.prepare_request(basereq)
        return request

    @retry(times=retry_times, backoff=backoff, exceptions=(Exception))
    def get_response(request_method):
        request = prepare_request(request_method)
        response = session.send(request, stream=True, timeout=timeout)
        response.raise_for_status()
        return response

    @retry(times=retry_times, backoff=backoff, exceptions=(Exception, ChunkedEncodingError))
    def run_simple():
        request = prepare_request(method)
        response = session.send(request, stream=True, timeout=timeout)
        response.raise_for_status()
        if progress_bar:
            pbar = tqdm(unit='B', unit_scale=True)
        with open(out_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    if progress_bar:
                        pbar.update(1024)
            f.close()
        if progress_bar:
            pbar.close()

    @retry(times=retry_times, backoff=backoff, exceptions=(Exception, ChunkedEncodingError))
    def run(file_size_, resume_=False):
        if osp.exists(out_file) and resume_:
            first_byte = osp.getsize(out_file)
            if first_byte == file_size:
                print("download was completed")
                return
            elif first_byte < file_size:
                print(f'resumming {out_file}...')
            else:
                print(f'file broken: {out_file}, re-download entire file')
                resume_ = False
                first_byte = 0
        else:
            first_byte = 0

        request = prepare_request(method)
        if first_byte > 0:
            request.headers['Range'] = f'bytes={first_byte}-'
            filemode = 'ab'
        else:
            filemode = 'wb'
        response = session.send(request, stream=True, timeout=timeout)
        response.raise_for_status()
        if first_byte > 0 and resume_:
            # check if status code is 206: Partial Content
            if response.status_code != 206:
                raise ValueError(f"Status code was {response.status_code}, not 206! The server does not support resuming.")

        if progress_bar:
            pbar = tqdm(total=file_size_, initial=first_byte, unit='B', unit_scale=True)

        with open(out_file, filemode) as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    if progress_bar:
                        pbar.update(1024)
            f.close()
        if progress_bar:
            pbar.close()
        # validate if download succeed
        if osp.getsize(out_file) != file_size_:
            raise ValueError(f"file size doesn't match content length from server: {out_file}")

    # main function body
    print(f"begin downloading {out_file}...")

    if resume:
        # check server for resuming support, reset to not resuming if it's not supported
        head_response = get_response('HEAD')
        if not head_response.headers.get('Accept-Ranges'):
            print("server doesn't support resuming, we will run download without resuming")
            resume = False
        if not head_response.headers.get('Content-Length'):
            print("content length missing. we will run download without resuming")
            resume = False
    if resume:
        file_size = int(head_response.headers['Content-Length'])
        run(file_size, resume)
    else:
        run_simple()


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
