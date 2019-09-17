
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

TBD

OECD Loader
-----------

TBD

Clio-infra Loader
-----------------

TBD
