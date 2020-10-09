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
    return any(x in scheme for x in ['http', 'https', 'file', 'ftp'] + ALL_SCHEMES)


def get_rev(name):
    if is_url(name):
        rev = name.split('@')[-1]
        if not rev:
            return 'master'
        return rev


def local_path_from_url(url, dataset_dir):
    """return a local path corresponding to the url"""
    if is_url(url):
        rel_path = url.split(':', 1)[1].lower().split('@')[0][2:]
        return os.path.join(dataset_dir, rel_path)
    else:
        raise ValueError(f"not an url: {url}")


def local_path_from_requirement(name, dataset_dir):
    """return a local path corresponding to a requirement string"""
    if '@' in name:
        return os.path.join(dataset_dir, 'repos', name)
    return os.path.join(dataset_dir, 'repos', name + '@master')


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
    # backend: VCSBackend
    _backend: VCSBackend = attr.ib(init=False, default=None)
    _local_path: str = attr.ib(init=False, default=None)

    @classmethod
    def from_uri(cls, uri, dataset_dir):
        assert is_url(uri), f"not an url: {uri}"
        for s in ALL_SCHEMES:
            if s in uri:
                protocol = s
                break
        else:
            scheme = get_url_scheme(uri)
            raise ValueError(f"scheme not supported: {scheme}")
        revision = get_rev(uri)
        return cls(protocol, uri, revision, dataset_dir)

    @property
    def backend(self):
        return self._backend

    def set_backend(self, backend):
        self._backend = backend

    @property
    def local_path(self):
        if not self._local_path:
            self._local_path = local_path_from_url(self.url, self.dataset_dir)
        return self._local_path

    def local_path_exists(self):
        return os.path.exists(self.local_path)
