"""
Instrument data cleaning utilities for PRIDE project data.

Handles cases where the generic "instrument model" term (MS:1000031) is used
with the actual instrument name as a value, which doesn't fit the standard data model.

Case 1 - Duplicate entries:
    When both a specific instrument term (e.g., MS:1000449 "LTQ Orbitrap") AND
    MS:1000031 with value "LTQ Orbitrap" exist, remove the redundant MS:1000031.

Case 2 - Resolve generic to specific:
    When only MS:1000031 with a value exists, try to resolve it to the actual
    instrument accession from the MS ontology.
"""

import re
from typing import TYPE_CHECKING, Any

from ontology_resolver.ontology_helper import OntologyHelper

from usigrabber.utils import logger

if TYPE_CHECKING:
    from pronto.ontology import Ontology

# The generic "instrument model" accession
INSTRUMENT_MODEL_ACCESSION = "MS:1000031"


def _normalize_name(name: str) -> str:
    """Normalize instrument name for comparison (case-insensitive, stripped)."""
    return name.strip().lower()


def _normalize_for_fuzzy_match(name: str) -> str:
    """
    Aggressively normalize instrument name for fuzzy matching.

    Handles common variations in instrument naming:
    - Case differences (QTOF vs QTof)
    - Hyphen/slash differences (TOF-TOF vs TOF/TOF)
    - Parenthetical vendor names like "(Waters)"
    - Extra whitespace
    - Common abbreviations

    The approach is to remove all non-alphanumeric characters and lowercase,
    creating a canonical form like "qtofpremier" that matches regardless of
    how separators are used.
    """
    result = name.lower()

    # Remove parenthetical text like "(Waters)" or "(Thermo)"
    result = re.sub(r"\([^)]*\)", "", result)

    # Remove common suffixes that may be added
    result = re.sub(r"\b(analyzer|system|instrument|spectrometer)\b", "", result)

    # Remove all non-alphanumeric characters (hyphens, slashes, spaces, etc.)
    result = re.sub(r"[^a-z0-9]", "", result)

    return result


def _find_matching_specific_instrument(
    instruments: list[dict[str, Any]], target_value: str
) -> dict[str, Any] | None:
    """
    Find a specific instrument entry whose name matches the target value.

    Args:
        instruments: List of instrument CV parameter dicts
        target_value: The value from MS:1000031 to match against

    Returns:
        The matching instrument dict, or None if not found
    """
    normalized_target = _normalize_name(target_value)

    for instrument in instruments:
        accession = instrument.get("accession", "")
        # Skip the generic instrument model term
        if accession == INSTRUMENT_MODEL_ACCESSION:
            continue

        name = instrument.get("name", "")
        if _normalize_name(name) == normalized_target:
            return instrument

    return None


class InstrumentNameResolver:
    """
    Resolves instrument names to MS ontology accessions using a local ontology.

    Builds a name-to-accession index from the MS ontology for fast lookups.
    Uses OntologyHelper to load the ontology from cache.

    Args:
        ontology: Optional pre-loaded ontology for testing. If not provided,
                  the MS ontology will be loaded via OntologyHelper.
    """

    def __init__(self, ontology: "Ontology | None" = None):
        self._ontology_helper = OntologyHelper()
        self._preloaded_ontology = ontology
        # Exact match index: normalized_name -> (accession, original_name)
        self._name_to_accession: dict[str, tuple[str, str]] | None = None
        # Fuzzy match index: fuzzy_normalized_name -> (accession, original_name)
        self._fuzzy_index: dict[str, tuple[str, str]] | None = None
        self._initialized = False

    async def _ensure_initialized(self) -> None:
        """Load the MS ontology and build the name indexes."""
        if self._initialized:
            return

        try:
            if self._preloaded_ontology is not None:
                ms_ontology = self._preloaded_ontology
            else:
                ms_ontology = await self._ontology_helper.get_ontology("MS")

            # Build name-to-accession indexes
            self._name_to_accession = {}
            self._fuzzy_index = {}

            for term in ms_ontology.terms():
                if term.name:
                    # Exact match index
                    normalized = _normalize_name(term.name)
                    self._name_to_accession[normalized] = (term.id, term.name)

                    # Fuzzy match index
                    fuzzy_normalized = _normalize_for_fuzzy_match(term.name)
                    if fuzzy_normalized:
                        self._fuzzy_index[fuzzy_normalized] = (term.id, term.name)

            self._initialized = True
            logger.debug(
                f"Instrument resolver initialized with {len(self._name_to_accession)} exact "
                f"and {len(self._fuzzy_index)} fuzzy terms"
            )
        except Exception as e:
            logger.error(f"Failed to initialize instrument resolver: {e}")
            self._name_to_accession = {}
            self._fuzzy_index = {}
            self._initialized = True

    async def resolve(self, instrument_name: str) -> tuple[str, str] | None:
        """
        Resolve an instrument name to its MS ontology accession.

        Tries exact match first, then falls back to fuzzy matching.

        Args:
            instrument_name: The instrument name to resolve

        Returns:
            Tuple of (accession, canonical_name) if found, None otherwise
        """
        await self._ensure_initialized()

        if not self._name_to_accession:
            return None

        # Try exact match first
        normalized = _normalize_name(instrument_name)
        if normalized in self._name_to_accession:
            return self._name_to_accession[normalized]

        # Try fuzzy match
        if self._fuzzy_index:
            fuzzy_normalized = _normalize_for_fuzzy_match(instrument_name)
            if fuzzy_normalized in self._fuzzy_index:
                return self._fuzzy_index[fuzzy_normalized]

        return None


# Global resolver instance (lazy initialization)
_resolver: InstrumentNameResolver | None = None


def _get_resolver() -> InstrumentNameResolver:
    """Get or create the global instrument resolver."""
    global _resolver
    if _resolver is None:
        _resolver = InstrumentNameResolver()
    return _resolver


async def clean_instruments(
    instruments: list[dict[str, Any]], resolver: InstrumentNameResolver | None = None
) -> list[dict[str, Any]]:
    """
    Clean instrument data by handling MS:1000031 "instrument model" entries.

    This function:
    1. Removes MS:1000031 entries when a matching specific instrument exists
    2. Resolves MS:1000031 values to specific accessions when possible

    Args:
        instruments: List of instrument CV parameter dicts from PRIDE API
        resolver: Optional custom resolver for testing. If not provided,
                  uses the global resolver.

    Returns:
        Cleaned list of instrument dicts
    """
    if not instruments:
        return instruments

    cleaned: list[dict[str, Any]] = []
    generic_entries: list[dict[str, Any]] = []

    # First pass: separate generic (MS:1000031) from specific instruments
    for instrument in instruments:
        accession = instrument.get("accession", "")
        if accession == INSTRUMENT_MODEL_ACCESSION:
            generic_entries.append(instrument)
        else:
            cleaned.append(instrument)

    # Second pass: process generic entries
    if resolver is None:
        resolver = _get_resolver()

    for generic in generic_entries:
        value = generic.get("value")

        if not value:
            # No value, keep as-is (unusual but possible)
            cleaned.append(generic)
            continue

        # Check if there's already a specific instrument with matching name
        matching_specific = _find_matching_specific_instrument(cleaned, value)

        if matching_specific:
            # Case 1: Duplicate - skip the generic entry
            logger.debug(
                f"Removing duplicate instrument entry: MS:1000031 with value '{value}' "
                f"(matches {matching_specific.get('accession')})"
            )
            continue

        # Case 2: Try to resolve to specific accession
        resolved = await resolver.resolve(value)

        if resolved:
            accession, name = resolved
            logger.debug(f"Resolved instrument '{value}' to {accession} ({name})")
            # Replace with the resolved instrument
            cleaned.append(
                {
                    "@type": "CvParam",
                    "cvLabel": "MS",
                    "accession": accession,
                    "name": name,
                }
            )
        else:
            # Could not resolve, keep the original entry
            logger.warning(
                f"Could not resolve instrument model value '{value}' to specific accession"
            )
            cleaned.append(generic)

    return cleaned
