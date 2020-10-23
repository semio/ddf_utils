"""
commands
"""

import os
import logging
from ddf_utils.vcs.base import VersionControl, is_url


logger = logging.getLogger('Package')


def get(package, dataset_dir):
    vcs = VersionControl.from_uri(package, dataset_dir)
    # FIXME: auto detect backend
    # vcs.set_backend(GitBackend())
    vcs.clone()


def install(package, dataset_dir, prefix=None, force=False):
    if is_url(package):
        logger.info(f'package is an url, see if we need to clone')
        vcs = VersionControl.from_uri(package, dataset_dir)
        if vcs.local_path_exists():
            logger.info('local folder exists, not cloning')
        else:
            # vcs.set_backend(GitBackend())
            vcs.clone()
    else:
        vcs = VersionControl.from_requirement(package, dataset_dir)

    if not os.path.exists(os.path.join(vcs.local_path, 'datapackage.json')):
        raise OSError(f'datapackage.json not found in {vcs.local_path}! Not installing.')

    vcs.install(prefix=prefix, force=force)
