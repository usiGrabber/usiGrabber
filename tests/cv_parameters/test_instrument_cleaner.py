"""Tests for instrument data cleaning utilities."""

import asyncio
from pathlib import Path

import pytest
from pronto.ontology import Ontology

from usigrabber.cv_parameters.instrument_cleaner import (
    InstrumentNameResolver,
    _find_matching_specific_instrument,
    _normalize_for_fuzzy_match,
    _normalize_name,
    clean_instruments,
)

# Path to mini ontology fixture for testing
MINI_ONTOLOGY_PATH = Path(__file__).parent / "fixtures" / "ms_instruments_mini.obo"


@pytest.fixture(scope="module")
def mini_ontology() -> Ontology:
    """Load the mini MS ontology for testing."""
    return Ontology(MINI_ONTOLOGY_PATH)


@pytest.fixture(scope="module")
def resolver(mini_ontology: Ontology) -> InstrumentNameResolver:
    """Create a resolver with the mini ontology."""
    return InstrumentNameResolver(ontology=mini_ontology)


def run_async(coro):
    """Helper to run async code in sync tests."""
    return asyncio.run(coro)


class TestNormalizeName:
    """Tests for name normalization."""

    def test_lowercase(self):
        assert _normalize_name("LTQ Orbitrap") == "ltq orbitrap"

    def test_strip_whitespace(self):
        assert _normalize_name("  LTQ Orbitrap  ") == "ltq orbitrap"

    def test_empty_string(self):
        assert _normalize_name("") == ""


class TestNormalizeForFuzzyMatch:
    """Tests for fuzzy normalization.

    The normalization removes all non-alphanumeric characters to create
    a canonical form that matches regardless of separator usage.
    """

    def test_removes_parenthetical_vendor(self):
        """Should remove vendor names in parentheses."""
        assert _normalize_for_fuzzy_match("Q-Tof Global Ultima (Waters)") == "qtofglobalultima"

    def test_removes_hyphens(self):
        """Hyphens should be removed."""
        assert _normalize_for_fuzzy_match("TOF-TOF") == "toftof"
        assert _normalize_for_fuzzy_match("Q-Tof") == "qtof"

    def test_removes_slashes(self):
        """Slashes should be removed."""
        assert _normalize_for_fuzzy_match("TOF/TOF") == "toftof"

    def test_removes_common_suffixes(self):
        """Should remove 'analyzer', 'spectrometer', etc."""
        assert (
            _normalize_for_fuzzy_match("4800 Plus MALDI TOF-TOF Analyzer") == "4800plusmalditoftof"
        )
        assert _normalize_for_fuzzy_match("Linear Ion Trap Spectrometer") == "lineariontrap"

    def test_removes_whitespace(self):
        """Whitespace should be removed."""
        assert _normalize_for_fuzzy_match("LTQ   Orbitrap") == "ltqorbitrap"

    def test_qtof_variations_match(self):
        """Different Q-Tof spellings should normalize to the same value."""
        # "Qtof-Premier" and "Q-Tof Premier" should normalize the same
        input1 = _normalize_for_fuzzy_match("Qtof-Premier")
        input2 = _normalize_for_fuzzy_match("Q-Tof Premier")
        assert input1 == input2 == "qtofpremier"

    def test_combined_normalization(self):
        """Test that all normalizations work together."""
        # "4800 Plus MALDI TOF-TOF Analyzer" should match "4800 Plus MALDI TOF/TOF"
        input1 = _normalize_for_fuzzy_match("4800 Plus MALDI TOF-TOF Analyzer")
        input2 = _normalize_for_fuzzy_match("4800 Plus MALDI TOF/TOF")
        assert input1 == input2 == "4800plusmalditoftof"


class TestFindMatchingSpecificInstrument:
    """Tests for finding matching specific instruments."""

    def test_finds_exact_match(self):
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
        ]
        result = _find_matching_specific_instrument(instruments, "LTQ Orbitrap")
        assert result is not None
        assert result["accession"] == "MS:1000449"

    def test_finds_case_insensitive_match(self):
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
        ]
        result = _find_matching_specific_instrument(instruments, "ltq orbitrap")
        assert result is not None
        assert result["accession"] == "MS:1000449"

    def test_skips_generic_instrument_model(self):
        """Should not match against other MS:1000031 entries."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
        ]
        result = _find_matching_specific_instrument(instruments, "LTQ Orbitrap")
        assert result is None

    def test_returns_none_when_no_match(self):
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
        ]
        result = _find_matching_specific_instrument(instruments, "Q Exactive")
        assert result is None

    def test_handles_empty_list(self):
        result = _find_matching_specific_instrument([], "LTQ Orbitrap")
        assert result is None


class TestCleanInstruments:
    """Tests for instrument cleaning."""

    def test_removes_duplicate_generic_entry(self, resolver: InstrumentNameResolver):
        """When specific instrument and matching MS:1000031 exist, remove the generic."""
        instruments = [
            {
                "@type": "CvParam",
                "cvLabel": "MS",
                "accession": "MS:1000449",
                "name": "LTQ Orbitrap",
            },
            {
                "@type": "CvParam",
                "cvLabel": "MS",
                "accession": "MS:1000031",
                "name": "instrument model",
                "value": "LTQ Orbitrap",
            },
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000449"

    def test_removes_duplicate_case_insensitive(self, resolver: InstrumentNameResolver):
        """Duplicate detection should be case-insensitive."""
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "ltq orbitrap"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000449"

    def test_resolves_known_instrument(self, resolver: InstrumentNameResolver):
        """When MS:1000031 has a known instrument name, resolve it."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        # Should resolve to MS:1000449
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000449"
        assert result[0]["name"] == "LTQ Orbitrap"

    def test_keeps_unresolvable_instrument(self, resolver: InstrumentNameResolver):
        """When instrument name cannot be resolved, keep the original entry."""
        instruments = [
            {
                "accession": "MS:1000031",
                "name": "instrument model",
                "value": "Completely Made Up Instrument XYZ123",
            },
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        # Should keep original since it cannot be resolved
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000031"
        assert result[0]["value"] == "Completely Made Up Instrument XYZ123"

    def test_keeps_generic_without_value(self, resolver: InstrumentNameResolver):
        """Generic entry without value should be kept."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000031"

    def test_handles_empty_list(self, resolver: InstrumentNameResolver):
        result = run_async(clean_instruments([], resolver=resolver))
        assert result == []

    def test_preserves_order_of_specific_instruments(self, resolver: InstrumentNameResolver):
        """Specific instruments should maintain their order."""
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1001910", "name": "Orbitrap Elite"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        assert len(result) == 2
        assert result[0]["accession"] == "MS:1000449"
        assert result[1]["accession"] == "MS:1001910"

    def test_multiple_generic_entries(self, resolver: InstrumentNameResolver):
        """Handle multiple MS:1000031 entries correctly."""
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "Unknown Device"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        # First generic should be removed (duplicate), second kept but unresolved
        assert len(result) == 2
        assert result[0]["accession"] == "MS:1000449"
        assert result[1]["accession"] == "MS:1000031"
        assert result[1]["value"] == "Unknown Device"


class TestInstrumentNameResolver:
    """Tests for the InstrumentNameResolver class."""

    def test_resolves_known_instrument(self, resolver: InstrumentNameResolver):
        """Should resolve a known instrument name."""
        result = run_async(resolver.resolve("LTQ Orbitrap"))

        assert result is not None
        accession, name = result
        assert accession == "MS:1000449"
        assert name == "LTQ Orbitrap"

    def test_resolves_case_insensitive(self, resolver: InstrumentNameResolver):
        """Resolution should be case-insensitive."""
        result = run_async(resolver.resolve("ltq orbitrap"))

        assert result is not None
        accession, _ = result
        assert accession == "MS:1000449"

    def test_returns_none_for_unknown(self, resolver: InstrumentNameResolver):
        """Should return None for unknown instrument names."""
        result = run_async(resolver.resolve("Not A Real Instrument"))

        assert result is None

    def test_caches_ontology(self, mini_ontology: Ontology):
        """Subsequent calls should use cached ontology."""
        resolver = InstrumentNameResolver(ontology=mini_ontology)

        # First call initializes
        run_async(resolver.resolve("LTQ Orbitrap"))

        # Second call should use cache
        assert resolver._initialized is True
        result = run_async(resolver.resolve("Q Exactive"))

        assert result is not None
        assert result[0] == "MS:1001911"  # Q Exactive

    def test_fuzzy_match_qtof_premier(self, resolver: InstrumentNameResolver):
        """Test fuzzy matching for Q-Tof Premier variations."""
        # "Qtof-Premier" should match "Q-Tof Premier" (MS:1000632)
        result = run_async(resolver.resolve("Qtof-Premier"))

        assert result is not None
        assert result[0] == "MS:1000632"

    def test_fuzzy_match_maldi_tof_tof(self, resolver: InstrumentNameResolver):
        """Test fuzzy matching for MALDI TOF-TOF variations."""
        # "4800 Plus MALDI TOF-TOF Analyzer" should match "4800 Plus MALDI TOF/TOF"
        result = run_async(resolver.resolve("4800 Plus MALDI TOF-TOF Analyzer"))

        assert result is not None
        assert result[0] == "MS:1000652"

    def test_fuzzy_match_with_vendor_parenthetical(self, resolver: InstrumentNameResolver):
        """Test matching when vendor name is in parentheses."""
        # "Q-Tof Global Ultima (Waters)" - parentheses should be ignored
        # This should NOT match "Q-Tof Ultima" because "Global" is significant
        result = run_async(resolver.resolve("Q-Tof Ultima (Waters)"))

        assert result is not None
        assert result[0] == "MS:1000189"  # Q-Tof Ultima


class TestRealWorldExamples:
    """
    Tests based on real-world examples from PRIDE data.

    Examples from the issue:
    - MS:1000031 with value "Qtof-Premier"
    - MS:1000031 with value "QSTAR XL"
    - MS:1000031 with value "4800 Plus MALDI TOF-TOF Analyzer"
    - MS:1000031 with value "LC-ESI-linear iontrap tandem mass spectrometer"
    - MS:1000031 with value "Esquire HCT"
    - MS:1000031 with value "LTQ"
    - MS:1000031 with value "Q-TOF Premier"
    - MS:1000031 with value "Q-Tof Global Ultima (Waters)"
    """

    def test_example_with_duplicate_ltq(self, resolver: InstrumentNameResolver):
        """Example: Both LTQ specific term and MS:1000031 with value 'LTQ'"""
        instruments = [
            {"accession": "MS:1000447", "name": "LTQ"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        # Should remove the duplicate
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000447"

    def test_resolve_ltq(self, resolver: InstrumentNameResolver):
        """Resolve 'LTQ' to MS:1000447 via ontology."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        # Should resolve to MS:1000447
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000447"
        assert result[0]["name"] == "LTQ"

    def test_resolve_qtof_premier(self, resolver: InstrumentNameResolver):
        """Resolve 'Qtof-Premier' to MS:1000632 (Q-Tof Premier)."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "Qtof-Premier"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000632"
        assert result[0]["name"] == "Q-Tof Premier"

    def test_resolve_q_tof_premier(self, resolver: InstrumentNameResolver):
        """Resolve 'Q-TOF Premier' to MS:1000632 (Q-Tof Premier)."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "Q-TOF Premier"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000632"
        assert result[0]["name"] == "Q-Tof Premier"

    def test_resolve_qstar_xl(self, resolver: InstrumentNameResolver):
        """Resolve 'QSTAR XL' to MS:1000657."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "QSTAR XL"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000657"
        assert result[0]["name"] == "QSTAR XL"

    def test_resolve_4800_maldi_tof_tof(self, resolver: InstrumentNameResolver):
        """Resolve '4800 Plus MALDI TOF-TOF Analyzer' to MS:1000652."""
        instruments = [
            {
                "accession": "MS:1000031",
                "name": "instrument model",
                "value": "4800 Plus MALDI TOF-TOF Analyzer",
            },
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000652"
        assert result[0]["name"] == "4800 Plus MALDI TOF/TOF"

    def test_resolve_q_tof_ultima_with_vendor(self, resolver: InstrumentNameResolver):
        """Resolve 'Q-Tof Global Ultima (Waters)' - vendor parenthetical removed."""
        instruments = [
            {
                "accession": "MS:1000031",
                "name": "instrument model",
                "value": "Q-Tof Ultima (Waters)",
            },
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000189"
        assert result[0]["name"] == "Q-Tof Ultima"

    def test_unresolvable_keeps_original(self, resolver: InstrumentNameResolver):
        """When instrument cannot be resolved, keep original MS:1000031 entry."""
        instruments = [
            {
                "accession": "MS:1000031",
                "name": "instrument model",
                "value": "LC-ESI-linear iontrap tandem mass spectrometer",
            },
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        # This descriptive name likely won't match any ontology term
        # so we keep the original
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000031"
        assert result[0]["value"] == "LC-ESI-linear iontrap tandem mass spectrometer"

    def test_duplicate_with_ltq_orbitrap(self, resolver: InstrumentNameResolver):
        """
        Real case from issue: Both specific instrument AND generic with same name.

        "instruments": [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"}
        ]
        """
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
        ]

        result = run_async(clean_instruments(instruments, resolver=resolver))

        # Should remove the duplicate MS:1000031 entry
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000449"
        assert result[0]["name"] == "LTQ Orbitrap"
