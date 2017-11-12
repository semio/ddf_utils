"""APIs for chef"""

from ddf_utils.chef.cook import Chef


def run_recipe(fn, out_dir):
    """run the recipe file and serve result"""
    from ddf_utils.io import cleanup
    import os
    if os.path.exists(out_dir):
        cleanup(out_dir)
    else:
        os.mkdir(out_dir)

    chef = Chef.from_recipe(fn)
    chef.run(serve=True, out_dir=out_dir)
    return
