# -*- coding: utf-8 -*-

"""transform the CDIAC CO2 data set to DDF model"""

import os

import numpy as np
import pandas as pd

from ddf_utils.io import cleanup
from ddf_utils.str import to_concept_id
from ddf_utils.model.ddf import Concept, EntityDomain, Entity
from ddf_utils.package import create_datapackage
from ddf_utils.io import dump_json

from tempfile import mkdtemp


# configuration of file path
nation_file = './source/nation.csv'
global_file = './source/global.csv'
out_dir = mkdtemp()

def read_source(f, skip=0, **kwargs):
    df = pd.read_csv(f, **kwargs)
    # quick fix for malformed csv downloaded from data povider
    if df.columns[0] == 'Year"':
        df = df.rename(columns={'Year"': 'Year'})
    df.columns = list(map(lambda x: x.lower().replace('\n', ''), df.columns))
    df = df.iloc[skip:]  # skip first few rows of data

    return df


def get_concept_id(name):
    """return concept name for given indicator name.
    """
    if 'total ' in name.lower():
        return 'total_carbon_emissions'
    else:
        subtypes = [
            'gas fuel consumption', 'liquid fuel consumption', 'solid fuel consumption',
            'cement production', 'gas flaring', 'bunker fuels', 'per capita'
        ]
        for i in subtypes:
            if i in name.lower():
                return 'carbon_emissions_' + to_concept_id(i)
        # if nothing found, it should be a non measure concept.
        return to_concept_id(name)


def get_concept_name(concept):
    if concept.startswith('carbon_emissions'):
        n0 = 'Carbon Emissions'
        n1 = concept.replace('carbon_emissions_', '').replace('_', ' ').title()
        return n0 + ' From ' + n1
    else:
        return concept.replace('_', ' ').title()


def replace_negative(ser):
    '''Replacing negative numbers with zeros.

    It's a known issue that in some series there are nagetive values, which is not
    allowed in production/consumption indicators.
    '''
    ser.loc[ser < 0] = 0
    return ser


if __name__ == '__main__':

    # cleanup the output dir
    print('clear up the old files..')
    cleanup(out_dir)

    # generate the dataset
    print("generating dataset...")
    print("output path: ", out_dir)

    # read source data
    nation_data = read_source(nation_file, skip=3, na_values='.')
    global_data = read_source(global_file, skip=1, na_values='.')

    # fix year to int
    nation_data.year = nation_data.year.map(int)
    global_data.year = global_data.year.map(int)

    # fix nation name for hkg and mac. There is a typo in it.
    nation_data['nation'] = nation_data['nation'].map(
        lambda x: x.replace('ADMINSTRATIVE', 'ADMINISTRATIVE') if 'ADMINSTRATIVE' in x else x)

    # create the location domain
    locations = EntityDomain(id='location', entities=[])
    for n in nation_data['nation'].unique():
        ent_id = to_concept_id(n)
        ent = Entity(id=ent_id, sets=['nation'], domain='location', props={'name': n})
        locations.add_entity(ent)

    ent = Entity(id='world', sets=['global'], domain='location', props={'name': 'World'})
    locations.add_entity(ent)

    for s in locations.entity_sets:
        ents = locations.get_entity_set(s)
        df = pd.DataFrame.from_records([e.to_dict() for e in ents])
        df.to_csv(os.path.join(out_dir,
                               f"ddf--entities--location--{s}.csv"),
                  index=False)

    # add concepts
    concepts = dict()

    concepts_measure = np.r_[global_data.columns,
                             nation_data.columns]
    concepts_measure = list(set(concepts_measure))
    for c in concepts_measure:
        cid = get_concept_id(c)
        concepts[cid] = Concept(id=cid, concept_type='measure', props={'name': c})

    concepts_discrete = ['name', 'unit', 'description', 'domain']
    for c in concepts_discrete:
        concepts[c] = Concept(id=c, concept_type='string', props={'name': c.upper()})
    for c in ['nation', 'global']:
        concepts[c] = Concept(id=c, concept_type='entity_set', props={'name': c.upper(), 'domain': 'location'})
    for c in ['name', 'unit', 'description']:
        concepts[c] = Concept(id=c, concept_type='string', props={'name': c.upper()})
    concepts['year'] = Concept(id='year', concept_type='time', props={'name': 'Year'})
    concepts['location'] = Concept(id='location', concept_type='entity_domain', props={'name': 'Location'})

    df = pd.DataFrame.from_records([c.to_dict() for c in concepts.values()])
    df.sort_values(by=['concept_type', 'concept']).to_csv(os.path.join(out_dir, 'ddf--concepts.csv'), index=False)

    # datapoints
    nation_data['nation'] = nation_data['nation'].map(to_concept_id)
    nation_data = nation_data.set_index(['nation', 'year'])
    nation_data.columns = nation_data.columns.map(get_concept_id)
    for c in nation_data:
        df = nation_data[[c]]
        df.to_csv(os.path.join(out_dir, f'ddf--datapoints--{c}--by--nation--year.csv'))

    global_data['global'] = 'world'
    global_data = global_data.set_index(['global', 'year'])
    global_data.columns = global_data.columns.map(get_concept_id)
    for c in global_data:
        df = global_data[[c]]
        df.to_csv(os.path.join(out_dir, f'ddf--datapoints--{c}--by--global--year.csv'))

    # datapackage
    dp = create_datapackage(out_dir)
    dump_json(os.path.join(out_dir, 'datapackage.json'), dp)
