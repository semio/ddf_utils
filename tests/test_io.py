"""test ddf_utils.io"""

import os
from ddf_utils.io import download_csv
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
