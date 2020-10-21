"""
git functions
"""

import os
import logging
import re
from datetime import datetime, timezone
from ddf_utils.vcs.base import (
    VCSBackend, local_path_from_url, get_url_scheme, call_subprocess,
    vcs
)


logger = logging.getLogger('Git')


HASH_REGEX = re.compile('^[a-fA-F0-9]{40}$')


def looks_like_hash(sha):
    return bool(HASH_REGEX.match(sha))


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
                silent=True
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

    @classmethod
    def remote_url(cls, path):
        cmd = ['ls-remote', '--get-url', 'origin']
        return cls.run_command(cmd, cwd=path, silent=True)

    @classmethod
    def clone(cls, url, path):
        logger.info(f"cloning {url} into {path}")
        cmd = ['clone', '--progress', url, path]
        os.makedirs(path, exist_ok=False)
        cls.run_command(cmd, silent=True)

    @classmethod
    def export(cls, path, rev, target_dir):
        if not target_dir.endswith('/'):
            target_dir = target_dir + '/'

        cls.run_command(
            ['worktree', 'add', '-f', target_dir, rev],
            cwd=path,
            silent=True
        )
        os.remove(os.path.join(target_dir, '.git'))
        cls.run_command(
            ['worktree', 'prune'],
            cwd=path,
            silent=True
        )

    @classmethod
    def get_revision(cls, location, rev=None):
        if rev is None:
            rev = 'HEAD'
        current_rev = cls.run_command(
            ['rev-parse', rev], cwd=location, silent=True
        )
        return current_rev.strip()

    @classmethod
    def tag_or_sha(cls, location, rev):
        # Pass rev to pre-filter the list.
        output = cls.run_command(['show-ref', rev], cwd=location,
                                 extra_ok_returncodes=[1], silent=True)
        refs = {}
        for line in output.strip().splitlines():
            try:
                sha, ref = line.split()
            except ValueError:
                # Include the offending line to simplify troubleshooting if
                # this error ever occurs.
                raise ValueError('unexpected show-ref line: {!r}'.format(line))

            refs[ref] = sha

        # TODO: support remote branch and rev parameter is already the whole ref
        branch_ref = 'refs/heads/{}'.format(rev)
        tag_ref = 'refs/tags/{}'.format(rev)

        sha = refs.get(branch_ref)
        if sha is not None:
            return (sha, True)

        sha = refs.get(tag_ref)

        return (sha, False)

    @classmethod
    def get_commit_time(cls, location, rev):
        cmd = ['show', '-s', '--format=%cI', rev]
        output = cls.run_command(cmd, cwd=location, silent=True)
        time_str = output.strip()
        return datetime.fromisoformat(time_str).astimezone(timezone.utc)

    @classmethod
    def get_latest_tag(cls, location, rev='HEAD'):
        sha = cls.get_revision(location, rev)
        tag_cmd = ['describe', '--tags', '--abbrev=0', '--always', sha]
        tag = cls.run_command(tag_cmd, cwd=location, silent=True).strip()
        if tag == sha:
            return None
        return tag

    @classmethod
    def run_command(cls, cmd, **kwargs):
        if isinstance(cmd, str):
            sub_cmd = [cmd]
        else:
            sub_cmd = cmd
        return call_subprocess([cls.executable] + sub_cmd, **kwargs)


vcs.register(GitBackend)
