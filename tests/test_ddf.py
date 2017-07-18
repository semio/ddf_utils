"""test dataset functions"""

import os
import tempfile

def test_dataset():
    from ddf_utils.model.package import Datapackage

    dataset_path = os.path.join(os.path.dirname(__file__),
                                'datasets/ddf--gapminder--dummy_companies')

    ds = Datapackage(dataset_path).dataset

    conc = ds.concepts
    ent = ds.entities
    dps = ds.datapoints

    ent_foundation = ds.get_entity('foundation')
    assert 'is--foundation' in ent_foundation.columns

    dp = ds.get_datapoint_df('lines_of_code', ('company', 'anno'))
    assert 'lines_of_code' in dp.columns

    assert not ds.is_empty
    assert len(ds.indicators()) == 3

    ds.get_data_copy()

    path = tempfile.mkdtemp()
    ds.to_ddfcsv(path)
    str(ds)
