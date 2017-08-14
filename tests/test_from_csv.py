"""test create ddf dataset from csv"""

from ddf_utils.cli import from_csv
from click.testing import CliRunner
import os
from tempfile import mkdtemp
from ddf_utils.model import Datapackage


def test_from_csv():
    input_dir = os.path.join(os.path.dirname(__file__), 'raw_csv')
    out_dir = mkdtemp()

    test_runner = CliRunner()
    test_runner.invoke(from_csv, ['-i', input_dir, '-o', out_dir])

    d = Datapackage(out_dir).load()
    assert len(d.indicators()) == 31