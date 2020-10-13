"""
git functions
"""

import os
import shutil
import attr
from ddf_utils.vcs.base import (
    VCSBackend, local_path_from_url, get_url_scheme, call_subprocess
)


class GitBackend(VCSBackend):
    name = 'git'
    executable = 'git'

    def clone(self, url, path):
        cmd = ['clone', '--progress', url, path]
        os.makedirs(path, exist_ok=False)
        self.run_command(cmd)

    def checkout(self, path, rev):
        pass

    def export(self, path, rev, target_dir):
        if not target_dir.endswith('/'):
            target_dir = target_dir + '/'

        self.run_command(
            ['worktree', 'add', '-f', target_dir, rev],
            cwd=path
        )
        os.remove(os.path.join(target_dir, '.git'))
        self.run_command(
            ['worktree', 'prune'],
            cwd=path
        )

    def run_command(self, cmd, **kwargs):
        if isinstance(cmd, str):
            sub_cmd = [cmd]
        else:
            sub_cmd = cmd
        call_subprocess([self.executable] + sub_cmd, **kwargs)
