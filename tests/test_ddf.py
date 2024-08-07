"""test dataset functions"""

import os
import tempfile

from ddf_utils.model.package import DDFcsv

def test_dataset():

    dataset_path = os.path.join(os.path.dirname(__file__),
                                'chef/datasets/ddf--gapminder--dummy_companies')

    ds = DDFcsv.from_path(dataset_path).ddf

    conc = ds.concepts
    ent = ds.entities
    dps = ds.datapoints
    syms = ds.synonyms

    ent_foundation = ds.get_entities('company', 'foundation')
    for e in ent_foundation:
        d = e.to_dict()
        assert d['is--foundation'] == 'TRUE'

    dp = ds.get_datapoints('lines_of_code', ('company', 'anno')).data
    assert 'lines_of_code' in dp.columns

    # assert not ds.is_empty
    assert len(ds.indicators()) == 5

    assert ds.get_datapoints('indicator2', ('anno', 'project')).data.compute().shape[0] == 2
    # ds.get_data_copy()

    # path = tempfile.mkdtemp()
    # ds.to_ddfcsv(path)
    str(ds)

    str(syms['region'])


def test_dtype():
    dataset_path = os.path.join(os.path.dirname(__file__),
                                'chef/datasets/ddf--cme')

    ds = DDFcsv.from_path(dataset_path).ddf

    dp = ds.get_datapoints('imr_lower', ('country', 'year')).data
    assert dp['year'].dtype == 'object'
    assert dp['country'].dtype == 'category'


if __name__ == '__main__':
    test_dtype()
