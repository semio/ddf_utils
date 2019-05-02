"""APIs for chef"""

import coloredlogs

from . model.chef import Chef
from . model.ingredient import ingredient_from_dict


coloredlogs.install(level=None, fmt='%(asctime)s %(levelname)s %(message)s')


def run_recipe(fn, ddf_dir, out_dir):
    """run the recipe file and serve result"""
    from ddf_utils.io import cleanup
    import os
    if os.path.exists(out_dir):
        cleanup(out_dir)
    else:
        os.mkdir(out_dir)

    chef = Chef.from_recipe(fn, ddf_dir=ddf_dir)
    chef.run(serve=True, outpath=out_dir)
    return
