# -*- coding: utf-8 -*-

from ddf_utils.factory import (CDIACLoader, ClioInfraLoader, IGMELoader,
                               IHMELoader, ILOLoader, OECDLoader, WorldBankLoader)


def test_ihme():
    ihme = IHMELoader()
    ihme._make_query('cause', 376)
    ihme.has_newer_source(376)


def test_wdi():
    wdi = WorldBankLoader()
    wdi.has_newer_source('WDI', '2010-12-12')


def test_ilo():
    ilo = ILOLoader()
    ilo.has_newer_source('GDP_211P_NOC_NB_A', '2012-10-01')


def test_cdiac():
    cdiac = CDIACLoader()
    cdiac.has_newer_source('2012-10-01')


def test_other_factory():
    for loader in [IGMELoader, OECDLoader, ClioInfraLoader]:
        ld = loader()
        ld.load_metadata()
