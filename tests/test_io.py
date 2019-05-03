"""test ddf_utils.io"""

import os
import pandas as pd
import numpy as np
from ddf_utils.io import download_csv, serve_datapoint, open_google_spreadsheet
from tempfile import mkdtemp


def test_download_csv():
    urls = [
        'http://cdiac.ornl.gov/ftp/ndp030/CSV-FILES/nation.1751_2014.csv',
        'http://cdiac.ornl.gov/ftp/ndp030/CSV-FILES/nation.1751_2013.csv'
    ]
    temp_dir = mkdtemp()

    download_csv(urls, temp_dir)

    flist = [
        'nation.1751_2014.csv',
        'nation.1751_2013.csv'
    ]

    for f in flist:
        assert os.path.exists(os.path.join(temp_dir, f))


def test_serve_datapoint():
    tmpdir = mkdtemp()

    df = pd.DataFrame(np.random.randn(100, 4), columns=list('ABCD'))

    serve_datapoint(df, tmpdir, concept='D', by=['A', 'B', 'C'])


def test_open_google_spreadsheet():
    open_google_spreadsheet('1L290jf0JPbboHmJPQbxI5PZMClA7w8msvXTedwW7jJE')


def test_repo():
    from click.testing import CliRunner
    from ddf_utils.cli import ddf
    from ddf_utils.model.repo import Repo

    r = Repo('https://github.com/open-numbers/ddf--gapminder--co2_emission')
    r.name

    runner = CliRunner()
    # diff
    result = runner.invoke(ddf, args=['diff', '--git', '-C', r.local_path,
                                      '-o', os.path.join(r.local_path, 'etl/diff'),
                                      'HEAD', 'HEAD~1'])
    assert result.exit_code == 0
