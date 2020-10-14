"""
git functions
"""

import os
import shutil
import attr
from ddf_utils.vcs.base import (
    VCSBackend, local_path_from_url, get_url_scheme, call_subprocess,
    vcs
)


class GitBackend(VCSBackend):
    name = 'git'
    dirname = '.git'
    executable = 'git'
    schemes = (
        'git', 'git+http', 'git+https', 'git+ssh', 'git+git', 'git+file',
    )
    # Prevent the user's environment variables from interfering with pip:
    # see github.com/pypa/pip issues#1130
    unset_environ = ('GIT_DIR', 'GIT_WORK_TREE')
    default_arg_rev = 'HEAD'

    @classmethod
    def get_repository_root(cls, location):
        loc = super(GitBackend, cls).get_repository_root(location)
        if loc:
            return loc
        try:
            r = cls.run_command(
                ['rev-parse', '--show-toplevel'],
                cwd=location,
                log_failed_cmd=False,
            )
        except Exception:
            return None
        # except BadCommand:
        #     logger.debug("could not determine if %s is under git control "
        #                  "because git is not available", location)
        #     return None
        # except SubProcessError:
        #     return None
        return os.path.normpath(r.rstrip('\r\n'))

    @property
    def remote_url(self):
        cmd = ['config', '--get', 'remote.origin.url']
        return self.run_command(cmd)

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
        return call_subprocess([self.executable] + sub_cmd, **kwargs)


vcs.register(GitBackend)
