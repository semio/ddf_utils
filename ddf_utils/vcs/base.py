"""
VCS support base functions
"""

import logging
import os
import shutil
import subprocess

import attr


logger = logging.getLogger(__name__)


ALL_SCHEMES = ['ssh', 'git', 'hg', 'bzr', 'sftp', 'svn']


def get_url_scheme(url):
    # type: (Union[str, Text]) -> Optional[Text]
    if ':' not in url:
        return None
    return url.split(':', 1)[0].lower()


def is_url(name):
    # type: (Union[str, Text]) -> bool
    """
    Return true if the name looks like a URL.
    """
    scheme = get_url_scheme(name)
    if scheme is None:
        return False
    return scheme in ['http', 'https', 'file', 'ftp'] + ALL_SCHEMES


def local_path_from_url(url, dataset_dir):
    """return a local path corresponding to the url"""
    pass


@attr.s(auto_attribs=True)
class VCSBackend(object):
    name: str
    executable: str

    def clone(self, url, path):
        raise NotImplementedError

    def checkout(self, rev, path):
        raise NotImplementedError

    def export(self, rev, path, target_dir):
        raise NotImplementedError


@attr.s(auto_attribs=True)
class VersionControl(object):
    protocol: str
    url: str
    revision: str
    dataset_dir: str
    backend: VCSBackend
    _local_path: str = attr.ib(init=False)

    @classmethod
    def from_uri(cls, uri, dataset_dir):
        pass

    @property
    def local_path(self):
        pass

    def local_path_exists(self):
        pass
