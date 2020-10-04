"""
git functions
"""

import attr
from . base import VCSBackend


@attr.s(auto_attrib=True)
class GitBackend(VCSBackend):

    def clone(self, url, path):
        pass

    def checkout(self, rev, path):
        pass

    def export(self, rev, path, target_dir):
        pass
