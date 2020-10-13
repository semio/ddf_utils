"""
commands
"""

from ddf_utils.vcs.base import VersionControl, is_url
from ddf_utils.vcs.git import GitBackend


def get(package, dataset_dir):
    vcs = VersionControl.from_uri(package, dataset_dir)
    # FIXME: auto detect backend
    vcs.set_backend(GitBackend())
    vcs.clone()


def install(package, dataset_dir):
    if is_url(package):
        vcs = VersionControl.from_uri(package, dataset_dir)
    else:
        vcs = VersionControl.from_requirement(package, dataset_dir)
    vcs.set_backend(GitBackend())
    if vcs.local_path_exists():
        print('target folder exists, not cloning')
    else:
        vcs.set_backend(GitBackend())
        vcs.clone()
    vcs.install()
