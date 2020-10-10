"""
git functions
"""

import os
import attr
from ddf_utils.vcs.base import (
    VCSBackend, local_path_from_url, get_url_scheme, call_subprocess
)


class GitBackend(VCSBackend):
    name = 'git'
    executable = 'git'

    def clone(self, url, path):
        cmd = ['clone', url, path]
        os.makedirs(path, exist_ok=False)
        self.run_command(cmd)

    def checkout(self, rev, path):
        pass

    def export(self, rev, path, target_dir):
        pass

    def run_command(self, cmd, **kwargs):
        if isinstance(cmd, str):
            sub_cmd = [cmd]
        else:
            sub_cmd = cmd
        call_subprocess([self.executable] + sub_cmd, **kwargs)
