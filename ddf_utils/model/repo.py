# -*- coding: utf-8 -*-

"""
model for dataset repositories.
"""

import logging
import os.path as osp
import tempfile
from urllib.parse import urlparse

from git import Repo as GRepo

from .package import DataPackage

logger = logging.getLogger(__name__)


def is_url(r):
    if r.startswith('https://') or r.startswith('git://'):
        return True
    else:
        return False


class Repo:
    def __init__(self, uri, base_path=None):
        if is_url(uri):
            if base_path is None:
                base_path = tempfile.mkdtemp()
            sub_path = osp.splitext(urlparse(uri).path)[0][1:]

            # self._name = osp.basename(sub_path)
            self._name = sub_path
            self._local_path = osp.join(base_path, sub_path)
            if osp.exists(self._local_path):
                # TODO: maybe give error?
                self.__repo = GRepo(self._local_path)
            else:
                print('checking out repo {} to {}'.format(sub_path, base_path))
                self.__repo = GRepo.clone_from(uri, to_path=self._local_path)

        else:
            assert osp.exists(osp.abspath(uri)), 'the path "{}" does not exist'.format(uri)
            self._name = osp.basename(uri)
            self._local_path = uri
            self.__repo = GRepo(uri)

    @property
    def name(self):
        return self._name

    @property
    def local_path(self):
        return self._local_path

    def show_versions(self):
        if len(self.__repo.tags) > 0:
            print('available versions:')
            [print(tag.name) for tag in self.__repo.tags]
        else:
            print("no version for the repo yet")

    def to_datapackage(self, ref=None):
        """turn repo@ref into Datapackage"""
        if ref is not None:
            self.__repo.git.checkout(ref)

        return DataPackage(self.local_path)
