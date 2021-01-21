"""
VCS support base functions
"""

import logging
import os
import shutil
import subprocess
import urllib
from datetime import datetime
from typing import Union, Optional, Text, List, Iterable, Mapping, Any, Type

import attr

logger = logging.getLogger('Package')


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
    if '@' in scheme:
        scheme = scheme.split('@')[0]
    return scheme in ['ssh', 'http', 'https', 'file', 'ftp'] + vcs.all_schemes


def extract_url_rev(name):
    # TODO: if revision is a hash, change it to full length / a fixed length
    if is_url(name):
        url_and_rev = name.split('+', 1)[1]
        result = url_and_rev.split('@')
        if len(result) == 1:
            return (result[0], 'master')
        elif len(result) == 2:
            if is_url(result[0]):
                return (result[0], result[1])
            else:
                return (url_and_rev, 'master')
        else:
            return ('@'.join(result[:-1]), result[-1])


def dataset_repo_path(dataset_dir):
    return os.path.join(dataset_dir, 'repo')


def dataset_pkg_path(dataset_dir):
    return os.path.join(dataset_dir, 'pkg')


def local_rel_path_from_url(url):
    if is_url(url):
        scheme = get_url_scheme(url)
        if '@' in scheme:  # such as git@github.com:abc/xyz
            host = scheme.split('@', 1)[1]
            rel_path = os.path.join(host, url.split(':')[1].lower().split('@')[0])
        else:  # such as https://github.com/abc/xyz
            rel_path = url.split(':', 1)[1].lower().split('@')[0][2:]
        return rel_path
    else:
        raise ValueError(f"not an url: {url}")


def local_path_from_url(url, dataset_dir):
    """return a local path corresponding to the url"""
    rel_path = local_rel_path_from_url(url)
    return os.path.join(dataset_repo_path(dataset_dir), rel_path)


def local_path_from_requirement(name, dataset_dir):
    """return a local path corresponding to a requirement string"""
    if '@' not in name:
        return os.path.join(dataset_repo_path(dataset_dir), name)
    name = name.split('@')[0]
    return os.path.join(dataset_repo_path(dataset_dir), name)


def find_path_to_dp_from_repo_root(location, repo_root):
    # type: (str, str) -> Optional[str]
    """
    Find the path to `datapackage.json` by searching up the filesystem from `location`.
    Return the path to `datapackage.json` relative to `repo_root`.
    Return None if `datapackage.json` is in `repo_root` or cannot be found.
    """
    # find setup.py
    orig_location = location
    while not os.path.exists(os.path.join(location, 'datapackage.json')):
        last_location = location
        location = os.path.dirname(location)
        if location == last_location:
            # We've traversed up to the root of the filesystem without
            # finding setup.py
            logger.warning(
                "Could not find datapackage.json for directory %s (tried all "
                "parent directories)",
                orig_location,
            )
            return None

    if os.path.samefile(repo_root, location):
        return None

    return os.path.relpath(location, repo_root)


def call_subprocess(
        cmd,  # type: Union[List[str]]
        cwd=None,  # type: Optional[str]
        extra_environ=None,  # type: Optional[Mapping[str, Any]]
        extra_ok_returncodes=None,  # type: Optional[Iterable[int]]
        log_failed_cmd=True,  # type: Optional[bool]
        silent=False
):
    # type: (...) -> Text
    """
    Args:
      extra_ok_returncodes: an iterable of integer return codes that are
        acceptable, in addition to 0. Defaults to None, which means [].
      log_failed_cmd: if false, failed commands are not logged,
        only raised.
      silent: if true, don't print any output to console
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
            universal_newlines=True
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
        if not silent:
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
    return ''.join(all_output).strip()


class VCSBackend(object):
    name: str
    executable: str
    dirname: str
    schemes: List[str]

    def clone(self, url, path):
        raise NotImplementedError

    def checkout(self, rev, path):
        raise NotImplementedError

    def export(self, rev, path, target_dir):
        raise NotImplementedError

    @classmethod
    def run_command(cmd):
        raise NotImplementedError

    @classmethod
    def get_repository_root(cls, location):
        # type: (str) -> Optional[str]
        """
        Return the "root" (top-level) directory controlled by the vcs,
        or `None` if the directory is not in any.

        It is meant to be overridden to implement smarter detection
        mechanisms for specific vcs.

        This can do more than is_repository_directory() alone. For
        example, the Git override checks that Git is actually available.
        """
        if cls.is_repository_directory(location):
            return location
        return None

    @classmethod
    def is_repository_directory(cls, path):
        # type: (str) -> bool
        """
        Return whether a directory path is a repository directory.
        """
        logger.debug('Checking in %s for %s (%s)...',
                     path, cls.dirname, cls.name)
        return os.path.exists(os.path.join(path, cls.dirname))


@attr.s()
class VcsSupport:
    schemes: list = attr.ib(
        default=['ssh', 'git', 'hg', 'bzr', 'sftp', 'svn'], init=False)
    _registry: dict = attr.ib(factory=dict, init=False)

    def __attrs_post_init__(self):
        urllib.parse.uses_netloc.extend(self.schemes)
        urllib.parse.uses_fragment.extend(self.schemes)

    @property
    def backends(self):
        # type: () -> List[VCSBackend]
        return list(self._registry.values())

    @property
    def dirnames(self):
        # type: () -> List[str]
        return [backend.dirname for backend in self.backends]

    @property
    def all_schemes(self):
        # type: () -> List[str]
        schemes = []  # type: List[str]
        for backend in self.backends:
            schemes.extend(backend.schemes)
        return schemes

    def register(self, cls):
        # type: (Type[VersionControl]) -> None
        if not hasattr(cls, 'name'):
            logger.warning('Cannot register VCS %s', cls.__name__)
            return
        if cls.name not in self._registry:
            self._registry[cls.name] = cls()
            logger.debug('Registered VCS backend: %s', cls.name)

    def unregister(self, name):
        # type: (str) -> None
        if name in self._registry:
            del self._registry[name]

    def get_backend_for_dir(self, location):
        # type: (str) -> Optional[VersionControl]
        """
        Return a VersionControl object if a repository of that type is found
        at the given directory.
        """
        vcs_backends = {}
        for vcs_backend in self._registry.values():
            repo_path = vcs_backend.get_repository_root(location)
            if not repo_path:
                continue
            logger.debug('Determine that %s uses VCS: %s',
                         location, vcs_backend.name)
            vcs_backends[repo_path] = vcs_backend

        if not vcs_backends:
            return None

        # Choose the VCS in the inner-most directory. Since all repository
        # roots found here would be either `location` or one of its
        # parents, the longest path should have the most path components,
        # i.e. the backend representing the inner-most repository.
        inner_most_repo_path = max(vcs_backends, key=len)
        return vcs_backends[inner_most_repo_path]

    def get_backend_for_scheme(self, scheme):
        # type: (str) -> Optional[VersionControl]
        """
        Return a VersionControl object or None.
        """
        for vcs_backend in self._registry.values():
            if scheme in vcs_backend.schemes:
                return vcs_backend
        return None

    def get_backend(self, name):
        # type: (str) -> Optional[VersionControl]
        """
        Return a VersionControl object or None.
        """
        name = name.lower()
        return self._registry.get(name)

    def get_repository_root(self, location):
        return (self.get_backend_for_dir(location)
                .get_repository_root(location))


vcs = VcsSupport()


@attr.s(auto_attribs=True)
class VersionControl(object):
    protocol: str
    url: str
    revision: str
    dataset_dir: str
    # backend: VCSBackend
    _backend: VCSBackend = attr.ib(init=False, default=None)
    _local_path: str = attr.ib(init=False, default=None)
    _package_name: str = attr.ib(init=False, default=None)

    @classmethod
    def from_uri(cls, uri, dataset_dir):
        assert is_url(uri), f"not an url: {uri}"
        for s in vcs.all_schemes:
            if s in uri:
                protocol = s
                break
        else:
            scheme = get_url_scheme(uri)
            raise ValueError(f"scheme not supported: {scheme}")
        url, rev = extract_url_rev(uri)
        res = cls(protocol, url, rev, dataset_dir)
        res.set_backend(vcs.get_backend(protocol))
        return res

    @classmethod
    def from_requirement(cls, package, dataset_dir):
        if '@' in package:
            package, rev = package.split('@', 1)
        else:
            rev = 'HEAD'
        if not os.path.isabs(package):
            if package == '.':
                full_path = os.path.abspath(package)
            else:
                if os.path.exists(os.path.abspath(package)):
                    full_path = os.path.abspath(package)
                elif os.path.exists(os.path.join(dataset_repo_path(dataset_dir), package)):
                    full_path = os.path.join(dataset_repo_path(dataset_dir), package)
                # shortcut for open-numbers
                elif os.path.exists(os.path.join(dataset_repo_path(dataset_dir),
                                                 'github.com', package)):
                    full_path = os.path.join(dataset_repo_path(dataset_dir),
                                             'github.com', package)
                else:
                    raise OSError(f"Couldn't find package {package} in "
                                  "current working dir and $DATASET_DIR!")
        else:
            full_path = package
            if not os.path.exists(full_path):
                raise OSError(f"Couldn't find package {package}!")

        protocol = 'local'
        url = 'file://' + full_path
        result = cls(protocol, url, rev, dataset_dir)
        if vcs.get_backend_for_dir(full_path):
            result._local_path = vcs.get_repository_root(full_path)
        else:
            # FIXME: detect the upper level folder which contains datapackage.json
            result._local_path = full_path
            result._package_name = os.path.basename(os.path.normpath(full_path))
        return result

    @classmethod
    def from_dataset_string(cls, dataset, dataset_dir):
        if is_url(dataset):
            return cls.from_uri(dataset, dataset_dir)
        else:
            try:
                return cls.from_requirement(dataset, dataset_dir)
            except OSError:
                # don't do anything, dataset repo not in the repo path, but
                # the dataset might be installed in pkg path.
                return None

    @property
    def backend(self):
        if not self._backend:
            self._backend = vcs.get_backend_for_dir(self.local_path)
        return self._backend

    def set_backend(self, backend):
        # TODO: add a backend list (registry)
        self._backend = backend

    @property
    def local_path(self):
        if not self._local_path:
            self._local_path = local_path_from_url(self.url, self.dataset_dir)
        return self._local_path

    @property
    def package_name(self):
        if not self._package_name:
            if self.protocol == 'local':
                remote_url = self.backend.remote_url(self.local_path)
                # TODO: if remote url haven't been configured?
                self._package_name = local_rel_path_from_url(remote_url)
            else:
                self._package_name = local_rel_path_from_url(self.url)
        return self._package_name

    def local_path_exists(self):
        return os.path.exists(self.local_path)

    def clone(self, custom_path=None):
        if not custom_path:
            self.backend.clone(self.url, self.local_path)
        else:
            relpath = os.path.relpath(self.dataset_dir, self.local_path)
            self.backend.clone(self.url, os.path.join(custom_path, relpath))

    def install(self, prefix=None, force=False):
        def make_pseudo_version(base, sha, tag=None):
            if not base:
                base = 'v0.0.0'
            if sha:
                time = self.backend.get_commit_time(self.local_path, sha)
                time_str = time.strftime('%Y%m%d%H%M%S')
            else:
                time = datetime.today()
                time_str = time.strftime('%Y%m%d%H%M%S')
            if sha:
                if tag:
                    return f'{base}-{time_str}-{sha[:12]}+{tag}'
                return f'{base}-{time_str}-{sha[:12]}'
            else:
                if tag:
                    return f'{base}-{time_str}+{tag}'
                return f'{base}-{time_str}+no_commit'

        if prefix:
            base_path = os.path.join(dataset_pkg_path(self.dataset_dir), prefix)
        else:
            base_path = dataset_pkg_path(self.dataset_dir)

        logger.info(f'installing {self.package_name} into {base_path}')
        if not self.backend:
            logger.warning('no backend detected')
            if self.revision == 'latest':
                pkg_rel_path = self.package_name + '@latest'
                pkg_path = os.path.join(base_path, pkg_rel_path)
                os.symlink(self.local_path, pkg_path)
            else:
                ver = make_pseudo_version(None, None, 'no_vcs')
                pkg_rel_path = self.package_name + '@' + ver
                pkg_path = os.path.join(base_path, pkg_rel_path)
                if not os.path.exists(pkg_path):
                    shutil.copytree(self.local_path, pkg_path)
                else:
                    if force:
                        shutil.rmtree(pkg_path)
                        shutil.copytree(self.local_path, pkg_path)
                    else:
                        logger.info(f'{pkg_rel_path} already installed')

        else:
            if self.revision == 'latest':  # symlink to @latest folder
                pkg_rel_path = self.package_name + '@latest'
                pkg_path = os.path.join(base_path, pkg_rel_path)
                os.symlink(self.local_path, pkg_path)
            else:  # do git export
                base_tag = self.backend.get_latest_tag(self.local_path, self.revision)
                sha, is_branch = self.backend.tag_or_sha(self.local_path, self.revision)
                if is_branch:
                    ver = make_pseudo_version(base_tag, sha)
                else:
                    if sha:
                        ver = self.revision
                    else:
                        sha = self.backend.get_revision(self.local_path, self.revision)
                        ver = make_pseudo_version(base_tag, sha)

                pkg_rel_path = self.package_name + '@' + ver
                pkg_path = os.path.join(base_path, pkg_rel_path)
                if not os.path.exists(pkg_path):
                    self.backend.export(self.local_path, self.revision, pkg_path)
                else:
                    if force:
                        shutil.rmtree(pkg_path)
                        self.backend.export(self.local_path, self.revision, pkg_path)
                    else:
                        logger.info(f'{pkg_rel_path} already installed')