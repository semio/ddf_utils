# -*- coding: utf-8 -*-

from ddf_utils.factory import (ClioInfraLoader, IHMELoader, ILOLoader, OECDLoader, WorldBankLoader)
from ddf_utils.factory.common import download
import tempfile


# def test_ihme():
#     ihme = IHMELoader()
#     ihme._make_query('cause', 376)
#     ihme.has_newer_source(376)

#     tmpdir = tempfile.mkdtemp()
#     test_url = 'http://semio.space/1m.txt'
#     ihme._run_download(test_url, tmpdir, '12345678')


# def test_wdi():
#     wdi = WorldBankLoader()
#     wdi.has_newer_source('WDI', '2010-12-12')


# def test_ilo():
#     ilo = ILOLoader()
#     ilo.has_newer_source('GDP_211P_NOC_NB_A', '2012-10-01')


# def test_other_factory():
#     for loader in [OECDLoader, ClioInfraLoader]:
#         ld = loader()
#         ld.load_metadata()


# def test_download_function():
#     tmpf = tempfile.mktemp()
#     test_url = 'http://semio.space/1m.txt'
#     download(test_url, tmpf, progress_bar=False)

#     tmpf = tempfile.mktemp()
#     test_url2 = 'https://unstats.un.org/SDGAPI/v1/sdg/Series/DataCSV'
#     download(test_url2, tmpf, method="POST", post_data={'seriesCodes': 'DC_ODA_BDVDL'}, progress_bar=False)
