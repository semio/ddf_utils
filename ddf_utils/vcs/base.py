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


def extract_url_rev(name):
    if is_url(name):
        url_and_rev = name.split('+', 1)[1]
        result = url_and_rev.split('@', 1)
        if len(result) == 1:
            return (result[0], 'master')
        return (result[0], result[1])


def local_path_from_url(url, dataset_dir):
    """return a local path corresponding to the url"""
    if is_url(url):
        rel_path = url.split(':', 1)[1].lower().split('@')[0][2:]
        return os.path.join(dataset_dir, 'repos', rel_path)
    else:
        raise ValueError(f"not an url: {url}")


def local_path_from_requirement(name, dataset_dir):
    """return a local path corresponding to a requirement string"""
    if '@' in name:
        return os.path.join(dataset_dir, 'repos', name)
    return os.path.join(dataset_dir, 'repos', name + '@master')


def call_subprocess(
        cmd,  # type: Union[List[str], CommandArgs]
        cwd=None,  # type: Optional[str]
        extra_environ=None,  # type: Optional[Mapping[str, Any]]
        extra_ok_returncodes=None,  # type: Optional[Iterable[int]]
        log_failed_cmd=True  # type: Optional[bool]
):
    # type: (...) -> Text
    """
    Args:
      extra_ok_returncodes: an iterable of integer return codes that are
        acceptable, in addition to 0. Defaults to None, which means [].
      log_failed_cmd: if false, failed commands are not logged,
        only raised.
    """
    if extra_ok_returncodes is None:
        extra_ok_returncodes = []

    # log the subprocess output at DEBUG level.
    # log_subprocess = subprocess_logger.debug

    env = os.environ.copy()
    if extra_environ:
        env.update(extra_environ)

    # Whether the subprocess will be visible in the console.
    # showing_subprocess = True

    # command_desc = format_command_args(cmd)
    try:
        proc = subprocess.Popen(
            # Convert HiddenText objects to the underlying str.
            # reveal_command_args(cmd),
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=cwd,
            text=True
        )
        if proc.stdin:
            proc.stdin.close()
    except Exception as exc:
        # if log_failed_cmd:
        #     subprocess_logger.critical(
        #         "Error %s while executing command %s", exc, command_desc,
        #     )
        raise
    all_output = []
    while True:
        # The "line" value is a unicode string in Python 2.
        line = None
        if proc.stdout:
            line = proc.stdout.readline()
        if not line:
            break
        line = line.rstrip()
        all_output.append(line + '\n')

        # Show the line immediately.
        print(line)
    try:
        proc.wait()
    finally:
        if proc.stdout:
            proc.stdout.close()

    proc_had_error = (
        proc.returncode and proc.returncode not in extra_ok_returncodes
    )
    if proc_had_error:
        # if not showing_subprocess and log_failed_cmd:
        #     # Then the subprocess streams haven't been logged to the
        #     # console yet.
        #     msg = make_subprocess_output_error(
        #         cmd_args=cmd,
        #         cwd=cwd,
        #         lines=all_output,
        #         exit_status=proc.returncode,
        #     )
        #     subprocess_logger.error(msg)
        # exc_msg = (
        #     'Command errored out with exit status {}: {} '
        #     'Check the logs for full command output.'
        # ).format(proc.returncode, command_desc)
        # raise SubProcessError(exc_msg)
        raise ValueError(f'command {cmd} failed with exit code: {proc.returncode}')
    return ''.join(all_output)


class VCSBackend(object):
    name: str
    executable: str

    def clone(self, url, path):
        raise NotImplementedError

    def checkout(self, rev, path):
        raise NotImplementedError

    def export(self, rev, path, target_dir):
        raise NotImplementedError

    def run_command(cmd):
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
        url, rev = extract_url_rev(uri)
        return cls(protocol, url, rev, dataset_dir)

    @property
    def backend(self):
        if self._backend:
            return self._backend
        else:
            raise ValueError("please set a backend first.")

    def set_backend(self, backend):
        # TODO: add a backend list (registry)
        self._backend = backend

    @property
    def local_path(self):
        if not self._local_path:
            self._local_path = local_path_from_url(self.url, self.dataset_dir)
        return self._local_path

    def local_path_exists(self):
        return os.path.exists(self.local_path)

    def clone(self, custom_path=None):
        if not custom_path:
            self.backend.clone(self.url, self.local_path)
        else:
            relpath = os.path.relpath(self.dataset_dir, self.local_path)
            self.backend.clone(self.url, os.path.join(custom_path, relpath))
