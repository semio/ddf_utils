# -*- coding: utf-8 -*-

from ddf_utils.factory import (CDIACLoader, ClioInfraLoader, IGMELoader,
                               IHMELoader, ILOLoader, OECDLoader, WorldBankLoader)

def test_factory():
    for loader in [CDIACLoader, ClioInfraLoader, IGMELoader,
                   IHMELoader, ILOLoader, OECDLoader, WorldBankLoader]:
        ld = loader()
        ld.load_metadata()
