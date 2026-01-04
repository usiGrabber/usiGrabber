"""Tests for instrument data cleaning utilities."""

import asyncio

import pytest

from usigrabber.cv_parameters.instrument_cleaner import (
    InstrumentNameResolver,
    _find_matching_specific_instrument,
    _normalize_for_fuzzy_match,
    _normalize_name,
    clean_instruments,
    clean_instruments_sync,
)


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


class TestCleanInstrumentsSync:
    """Tests for synchronous instrument cleaning (Case 1 only - duplicates)."""

    def test_removes_duplicate_generic_entry(self):
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

        result = clean_instruments_sync(instruments)

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000449"

    def test_removes_duplicate_case_insensitive(self):
        """Duplicate detection should be case-insensitive."""
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "ltq orbitrap"},
        ]

        result = clean_instruments_sync(instruments)

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000449"

    def test_keeps_generic_when_no_duplicate(self):
        """When no matching specific instrument, keep the generic entry."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "Unknown Instrument"},
        ]

        result = clean_instruments_sync(instruments)

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000031"
        assert result[0]["value"] == "Unknown Instrument"

    def test_keeps_generic_without_value(self):
        """Generic entry without value should be kept."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model"},
        ]

        result = clean_instruments_sync(instruments)

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000031"

    def test_handles_empty_list(self):
        assert clean_instruments_sync([]) == []

    def test_preserves_order_of_specific_instruments(self):
        """Specific instruments should maintain their order."""
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1001910", "name": "Orbitrap Elite"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
        ]

        result = clean_instruments_sync(instruments)

        assert len(result) == 2
        assert result[0]["accession"] == "MS:1000449"
        assert result[1]["accession"] == "MS:1001910"

    def test_multiple_generic_entries(self):
        """Handle multiple MS:1000031 entries correctly."""
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "Unknown Device"},
        ]

        result = clean_instruments_sync(instruments)

        # First generic should be removed (duplicate), second should be kept
        assert len(result) == 2
        assert result[0]["accession"] == "MS:1000449"
        assert result[1]["accession"] == "MS:1000031"
        assert result[1]["value"] == "Unknown Device"


class TestCleanInstrumentsAsync:
    """Tests for async instrument cleaning (includes Case 2 - resolution)."""

    def test_removes_duplicate_generic_entry(self):
        """Same as sync version - removes duplicates."""
        instruments = [
            {"accession": "MS:1000449", "name": "LTQ Orbitrap"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
        ]

        result = run_async(clean_instruments(instruments))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000449"

    def test_handles_empty_list(self):
        result = run_async(clean_instruments([]))
        assert result == []

    @pytest.mark.slow
    def test_resolves_known_instrument(self):
        """When MS:1000031 has a known instrument name, resolve it."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ Orbitrap"},
        ]

        result = run_async(clean_instruments(instruments))

        # Should resolve to MS:1000449
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000449"
        assert result[0]["name"] == "LTQ Orbitrap"

    @pytest.mark.slow
    def test_keeps_unresolvable_instrument(self):
        """When instrument name cannot be resolved, keep the original entry."""
        instruments = [
            {
                "accession": "MS:1000031",
                "name": "instrument model",
                "value": "Completely Made Up Instrument XYZ123",
            },
        ]

        result = run_async(clean_instruments(instruments))

        # Should keep original since it cannot be resolved
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000031"
        assert result[0]["value"] == "Completely Made Up Instrument XYZ123"


class TestInstrumentNameResolver:
    """Tests for the InstrumentNameResolver class."""

    @pytest.mark.slow
    def test_resolves_known_instrument(self):
        """Should resolve a known instrument name."""
        resolver = InstrumentNameResolver()

        result = run_async(resolver.resolve("LTQ Orbitrap"))

        assert result is not None
        accession, name = result
        assert accession == "MS:1000449"
        assert name == "LTQ Orbitrap"

    @pytest.mark.slow
    def test_resolves_case_insensitive(self):
        """Resolution should be case-insensitive."""
        resolver = InstrumentNameResolver()

        result = run_async(resolver.resolve("ltq orbitrap"))

        assert result is not None
        accession, _ = result
        assert accession == "MS:1000449"

    @pytest.mark.slow
    def test_returns_none_for_unknown(self):
        """Should return None for unknown instrument names."""
        resolver = InstrumentNameResolver()

        result = run_async(resolver.resolve("Not A Real Instrument"))

        assert result is None

    @pytest.mark.slow
    def test_caches_ontology(self):
        """Subsequent calls should use cached ontology."""
        resolver = InstrumentNameResolver()

        # First call initializes
        run_async(resolver.resolve("LTQ Orbitrap"))

        # Second call should use cache
        assert resolver._initialized is True
        result = run_async(resolver.resolve("Q Exactive"))

        assert result is not None
        assert result[0] == "MS:1001911"  # Q Exactive

    @pytest.mark.slow
    def test_fuzzy_match_qtof_premier(self):
        """Test fuzzy matching for Q-Tof Premier variations."""
        resolver = InstrumentNameResolver()

        # "Qtof-Premier" should match "Q-Tof Premier" (MS:1000632)
        result = run_async(resolver.resolve("Qtof-Premier"))

        assert result is not None
        assert result[0] == "MS:1000632"

    @pytest.mark.slow
    def test_fuzzy_match_maldi_tof_tof(self):
        """Test fuzzy matching for MALDI TOF-TOF variations."""
        resolver = InstrumentNameResolver()

        # "4800 Plus MALDI TOF-TOF Analyzer" should match "4800 Plus MALDI TOF/TOF"
        result = run_async(resolver.resolve("4800 Plus MALDI TOF-TOF Analyzer"))

        assert result is not None
        assert result[0] == "MS:1000652"

    @pytest.mark.slow
    def test_fuzzy_match_with_vendor_parenthetical(self):
        """Test matching when vendor name is in parentheses."""
        resolver = InstrumentNameResolver()

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

    def test_example_with_duplicate_ltq(self):
        """Example: Both LTQ specific term and MS:1000031 with value 'LTQ'"""
        instruments = [
            {"accession": "MS:1000447", "name": "LTQ"},
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ"},
        ]

        result = clean_instruments_sync(instruments)

        # Should remove the duplicate
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000447"

    def test_sync_keeps_unresolvable_instruments(self):
        """Sync version keeps instruments it cannot resolve."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "Esquire HCT"},
        ]

        result = clean_instruments_sync(instruments)
        # Sync cannot resolve, so keeps original
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000031"

    @pytest.mark.slow
    def test_resolve_ltq(self):
        """Resolve 'LTQ' to MS:1000447 via ontology."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "LTQ"},
        ]

        result = run_async(clean_instruments(instruments))

        # Should resolve to MS:1000447
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000447"
        assert result[0]["name"] == "LTQ"

    @pytest.mark.slow
    def test_resolve_qtof_premier(self):
        """Resolve 'Qtof-Premier' to MS:1000632 (Q-Tof Premier)."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "Qtof-Premier"},
        ]

        result = run_async(clean_instruments(instruments))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000632"
        assert result[0]["name"] == "Q-Tof Premier"

    @pytest.mark.slow
    def test_resolve_q_tof_premier(self):
        """Resolve 'Q-TOF Premier' to MS:1000632 (Q-Tof Premier)."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "Q-TOF Premier"},
        ]

        result = run_async(clean_instruments(instruments))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000632"
        assert result[0]["name"] == "Q-Tof Premier"

    @pytest.mark.slow
    def test_resolve_qstar_xl(self):
        """Resolve 'QSTAR XL' to MS:1000657."""
        instruments = [
            {"accession": "MS:1000031", "name": "instrument model", "value": "QSTAR XL"},
        ]

        result = run_async(clean_instruments(instruments))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000657"
        assert result[0]["name"] == "QSTAR XL"

    @pytest.mark.slow
    def test_resolve_4800_maldi_tof_tof(self):
        """Resolve '4800 Plus MALDI TOF-TOF Analyzer' to MS:1000652."""
        instruments = [
            {
                "accession": "MS:1000031",
                "name": "instrument model",
                "value": "4800 Plus MALDI TOF-TOF Analyzer",
            },
        ]

        result = run_async(clean_instruments(instruments))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000652"
        assert result[0]["name"] == "4800 Plus MALDI TOF/TOF"

    @pytest.mark.slow
    def test_resolve_q_tof_ultima_with_vendor(self):
        """Resolve 'Q-Tof Global Ultima (Waters)' - vendor parenthetical removed."""
        instruments = [
            {
                "accession": "MS:1000031",
                "name": "instrument model",
                "value": "Q-Tof Ultima (Waters)",
            },
        ]

        result = run_async(clean_instruments(instruments))

        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000189"
        assert result[0]["name"] == "Q-Tof Ultima"

    @pytest.mark.slow
    def test_unresolvable_keeps_original(self):
        """When instrument cannot be resolved, keep original MS:1000031 entry."""
        instruments = [
            {
                "accession": "MS:1000031",
                "name": "instrument model",
                "value": "LC-ESI-linear iontrap tandem mass spectrometer",
            },
        ]

        result = run_async(clean_instruments(instruments))

        # This descriptive name likely won't match any ontology term
        # so we keep the original
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000031"
        assert result[0]["value"] == "LC-ESI-linear iontrap tandem mass spectrometer"

    @pytest.mark.slow
    def test_duplicate_with_ltq_orbitrap(self):
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

        result = run_async(clean_instruments(instruments))

        # Should remove the duplicate MS:1000031 entry
        assert len(result) == 1
        assert result[0]["accession"] == "MS:1000449"
        assert result[0]["name"] == "LTQ Orbitrap"
