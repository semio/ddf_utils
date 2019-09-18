
========================================
 Downloading Data from Source Providers
========================================

ddf_utils provides a few classes under ``ddf_utils.factory`` module to
help downloading data from serval data providers. Currently we support
downloading files from `Clio-infra`_, `IHME GBD`_, ILOStat_, OECD_ and
WorldBank_. Whenever possible we will use bulk download method from
the provider.

.. _IHME GBD: http://ghdx.healthdata.org/gbd-results-tool
.. _ILOstat: https://www.ilo.org/ilostat
.. _OECD: https://stats.oecd.org/
.. _WorldBank: https://data.worldbank.org/
.. _`Clio-infra`: https://www.clio-infra.eu/index.html

General Interface
-----------------
We created a general class for these data loaders, which has a
``metadata`` property and ``load_metadata``, ``has_newer_source`` and
``bulk_download`` methods.

``metadata`` is for all kinds of metadata provided by data source, such
as dimension list and available values in a dimension. ``load_metadata``
tries to load these metadata. ``has_newer_source`` tries to find out if
there is newer version of source data available. And ``bulk_download``
download the requested data files.

IHME GBD Loader
---------------

.. note::

   The API we use in the IHME loader was not documented anywhere in
   the IHME website. So there may be problems in the loader.

The IHME GBD loader works in the same way as the GBD Result Tool. Just
like one would do a search in the result tool, we need to select
context/country/age etc. In IHME loader, we should provide a
dictionary of query parameters to the :py:meth:`bulk_download
<ddf_utils.factory.ihme.IHMELoader.bulk_download>` method (See the
docstring for bulk_download for the usage). And the values for them should be the
numeric IDs from IHME. We can check these ID from metadata.

Example Usage:

.. code-block:: ipython

   In [1]: from ddf_utils.factory.ihme import IHMELoader

   In [2]: l = IHMELoader()

   In [3]: md = l.load_metadata()

   In [4]: md.keys()
   Out[4]: dict_keys(['age', 'cause', 'groups', 'location', 'measure', 'metric', 'rei', 'sequela', 'sex', 'year', 'year_range', 'version'])

   In [5]: md['age'].head()
   Out[5]:
   id      name short_name  sort  plot       type
   1    1   Under 5         <5    22     0  aggregate
   10  10  25 to 29         25    10     1   specific
   11  11  30 to 34         30    11     1   specific
   12  12  35 to 39         35    12     1   specific
   13  13  40 to 44         40    13     1   specific

   In [6]: l.bulk_download('/tmp/', context='le', version=376, year=[2017], email='your-email@mailer.com')
   working on https://s3.healthdata.org/gbd-api-2017-public/xxxx
   check status as http://ghdx.healthdata.org/gbd-results-tool/result/xxxx
   available downloads:
   http://s3.healthdata.org/gbd-api-2017-public/xxxx_files/IHME-GBD_2017_DATA-03cf30ab-1.zip
   downloading http://s3.healthdata.org/gbd-api-2017-public/xxxx_files/IHME-GBD_2017_DATA-03cf30ab-1.zip to /tmp/xxxx/IHME-GBD_2017_DATA-xxxx-1.zip
   1.13MB [00:01, 582kB/s]
   Out[6]: ['03cf30ab']


ILOStat Loader
--------------

The ILO data loader use the `bulk download facility`_ from ILO.

See the `API doc`_ for how to use this loader.

.. _`bulk download facility`: http://www.ilo.org/ilostat-files/WEB_bulk_download/ILOSTAT_BulkDownload_Guidelines.pdf
.. _`API doc`: https://ddf-utils.readthedocs.io/en/dev/ddf_utils.factory.html#module-ddf_utils.factory.ilo

WorldBank Loader
----------------

The Worldbank loader can download all datasets listed in the `data catalog`_ in CSV(zip) format.

.. _`data catalog`: https://datacatalog.worldbank.org/

Example Usage:

.. code-block:: ipython

   In [1]: from ddf_utils.factory.worldbank import WorldBankLoader

   In [2]: w = WorldBankLoader()

   In [3]: md = w.load_metadata()

   In [4]: md.head()
   Out[4]:
                        accessoption acronym api                          apiaccessurl  ...
   0  API, Bulk download, Query tool     WDI   1  http://data.worldbank.org/developers  ...
   1  API, Bulk download, Query tool     ADI   1  http://data.worldbank.org/developers  ...
   2  API, Bulk download, Query tool     GEM   1  http://data.worldbank.org/developers  ...
   3                      Query tool     NaN   0                                   NaN  ...
   4  API, Bulk download, Query tool    MDGs   1  http://data.worldbank.org/developers  ...
   ...

   In [5]: w.bulk_download('MDGs', '/tmp/')
   Out[5]: '/tmp/'


OECD Loader
-----------

The OECD loader can download all datasets in `OECD stats`_. We use the SDMX-JSON api and the downloaded dataset will be in json file. Learn more about SDMX-JSON in the `OECD api doc`_.

.. _`OECD stats`: https://stats.oecd.org/
.. _`OECD api doc`: https://data.oecd.org/api/sdmx-json-documentation/

Example Usage:

.. code-block:: ipython

   In [1]: from ddf_utils.factory.oecd import OECDLoader

   In [2]: o = OECDLoader()

   In [3]: md = o.load_metadata()

   In [4]: # metadata contains all available datasets.

   In [5]: md.head()
   Out[5]:
   id                                               name
   0          QNA                        Quarterly National Accounts
   1      PAT_IND                                  Patent indicators
   2  SNA_TABLE11     11. Government expenditure by function (COFOG)
   3    EO78_MAIN  Economic Outlook No 78 - December 2005 - Annua...
   4        ANHRS    Average annual hours actually worked per worker

   In [6]: o.bulk_download('/tmp/', 'EO78_MAIN')

Clio-infra Loader
-----------------

The Clio infra loader parse the `home page for clio infra`_ and do bulk download for all datasets or all country profiles.

.. _`home page for clio infra`: https://clio-infra.eu/

Example Usage:

.. code-block:: ipython

   In [1]: from ddf_utils.factory.clio_infra import ClioInfraLoader

   In [2]: c = ClioInfraLoader()

   In [3]: md = c.load_metadata()

   In [4]: md.head()
   Out[4]:
                     name                                     url     type
   0    Cattle per Capita    ../data/CattleperCapita_Compact.xlsx  dataset
   1  Cropland per Capita  ../data/CroplandperCapita_Compact.xlsx  dataset
   2     Goats per Capita     ../data/GoatsperCapita_Compact.xlsx  dataset
   3   Pasture per Capita   ../data/PastureperCapita_Compact.xlsx  dataset
   4      Pigs per Capita      ../data/PigsperCapita_Compact.xlsx  dataset

   In [5]: md['type'].unique()
   Out[5]: array(['dataset', 'country'], dtype=object)

   In [6]: c.bulk_download('/tmp', data_type='dataset')
   downloading https://clio-infra.eu/data/CattleperCapita_Compact.xlsx to /tmp/Cattle per Capita.xlsx
   ...
