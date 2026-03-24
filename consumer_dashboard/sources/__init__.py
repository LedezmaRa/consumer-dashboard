"""Source adapters."""

from consumer_dashboard.sources.base import BaseSourceAdapter
from consumer_dashboard.sources.bea import BeaSourceAdapter
from consumer_dashboard.sources.bls import BlsSourceAdapter
from consumer_dashboard.sources.census import CensusSourceAdapter
from consumer_dashboard.sources.conference_board import ConferenceBoardSourceAdapter
from consumer_dashboard.sources.dol import DolSourceAdapter
from consumer_dashboard.sources.fed import FederalReserveBoardSourceAdapter
from consumer_dashboard.sources.michigan import MichiganSourceAdapter
from consumer_dashboard.sources.nar import NarSourceAdapter
from consumer_dashboard.sources.nyfed import NewYorkFedSourceAdapter

ADAPTERS = {
    "bea": BeaSourceAdapter,
    "bls": BlsSourceAdapter,
    "census": CensusSourceAdapter,
    "conference_board": ConferenceBoardSourceAdapter,
    "dol": DolSourceAdapter,
    "federal_reserve_board": FederalReserveBoardSourceAdapter,
    "michigan": MichiganSourceAdapter,
    "university_of_michigan": MichiganSourceAdapter,
    "new_york_fed": NewYorkFedSourceAdapter,
    "nar": NarSourceAdapter,
}


def build_adapter(source_id: str, settings) -> BaseSourceAdapter:
    key = source_id.strip().lower()
    if key not in ADAPTERS:
        available = ", ".join(sorted(ADAPTERS))
        raise KeyError(f"Unknown adapter '{source_id}'. Available adapters: {available}")
    return ADAPTERS[key](settings)
