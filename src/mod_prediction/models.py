"""Pydantic models for PROXI API responses and MGF format."""

from typing import Any, NotRequired, TypedDict

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, field_validator


class PyteomicsAttribute(BaseModel):
    """Attribute in pyteomics PROXI response."""

    model_config = ConfigDict(extra="allow")

    accession: str
    name: str
    value: str | int | float | None = None


class Spectrum(BaseModel):
    """Spectrum data from raw file extraction."""

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    mz_array: np.ndarray = Field(alias="mzs")
    intensity_array: np.ndarray = Field(alias="intensities")
    attributes: list[PyteomicsAttribute] = Field(default_factory=list)

    @field_validator("mz_array", "intensity_array", mode="before")
    @classmethod
    def validate_numpy_array(cls, v: Any) -> np.ndarray:
        """Ensure arrays are numpy arrays."""
        if isinstance(v, np.ndarray):
            return v
        if isinstance(v, list):
            return np.array(v, dtype=float)
        raise ValueError(f"Expected numpy array or list, got {type(v)}")

    def model_post_init(self, __context: Any) -> None:
        """Validate array lengths match."""
        if len(self.mz_array) != len(self.intensity_array):
            raise ValueError(
                f"Array length mismatch: "
                f"m/z array={len(self.mz_array)}, intensity array={len(self.intensity_array)}"
            )


class PyteomicsSpectrum(BaseModel):
    """Pyteomics PROXI response format with numpy array support."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    usi: str
    status: str
    attributes: list[PyteomicsAttribute]
    mz_array: np.ndarray = Field(alias="m/z array")
    intensity_array: np.ndarray = Field(alias="intensity array")

    @field_validator("mz_array", "intensity_array", mode="before")
    @classmethod
    def validate_numpy_array(cls, v: Any) -> np.ndarray:
        """Ensure arrays are numpy arrays."""
        if isinstance(v, np.ndarray):
            return v
        if isinstance(v, list):
            return np.array(v, dtype=float)
        raise ValueError(f"Expected numpy array or list, got {type(v)}")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Ensure status is READABLE."""
        if v != "READABLE":
            raise ValueError(f"Expected status 'READABLE', got '{v}'")
        return v

    def model_post_init(self, __context: Any) -> None:
        """Validate array lengths match."""
        if len(self.mz_array) != len(self.intensity_array):
            raise ValueError(
                f"Array length mismatch: "
                f"m/z array={len(self.mz_array)}, intensity array={len(self.intensity_array)}"
            )


class MGFParams(TypedDict):
    """Parameters for MGF spectrum format."""

    title: str
    scans: str
    charge: list[int]
    pepmass: NotRequired[tuple[float, None]]
    seq: NotRequired[str]
    rtinseconds: NotRequired[float]


class MGFSpectrum(BaseModel):
    """MGF spectrum format compatible with pyteomics.mgf.

    Note: Arrays use special dict key names with spaces as required by pyteomics.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    mz_array: np.ndarray = Field(alias="m/z array")
    intensity_array: np.ndarray = Field(alias="intensity array")
    params: MGFParams

    @field_validator("mz_array", "intensity_array", mode="before")
    @classmethod
    def validate_numpy_array(cls, v: Any) -> np.ndarray:
        """Ensure arrays are numpy arrays."""
        if isinstance(v, np.ndarray):
            return v
        if isinstance(v, list):
            return np.array(v, dtype=float)
        raise ValueError(f"Expected numpy array or list, got {type(v)}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict format expected by pyteomics.mgf.write()."""
        return {
            "m/z array": self.mz_array,
            "intensity array": self.intensity_array,
            "params": dict(self.params) if isinstance(self.params, BaseModel) else self.params,
        }


class PSMWithModifications(BaseModel):
    """PSM metadata with aggregated modifications (one record per psm_id)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Core PSM identifiers
    psm_id: str
    project_accession: str
    spectrum_id: str | None = None
    charge_state: int | None = None

    # Mass and quality metrics
    experimental_mz: float | None = None
    calculated_mz: float | None = None
    pass_threshold: bool | None = None
    rank: int | None = None

    # Scan identifiers
    ms_run: str
    index_number: int
    index_type: str

    # Peptide information
    peptide_sequence: str | None = None
    modified_peptide_id: str | None = None

    # Aggregated modifications (arrays)
    unimod_ids: list[int] = Field(default_factory=list)
    locations: list[int] = Field(default_factory=list)
    modified_residues: list[str | None] | None = Field(default=None)

    def model_post_init(self, __context: Any) -> None:
        """Validate modification arrays have matching lengths."""
        lengths: list[int] = [len(self.unimod_ids), len(self.locations)]
        if self.modified_residues is not None:
            lengths.append(len(self.modified_residues))
        if len(set[int](lengths)) > 1:
            raise ValueError(
                f"Modification array length mismatch: "
                f"unimod_ids={len(self.unimod_ids)}, "
                f"locations={len(self.locations)}, "
                f"modified_residues={len(self.modified_residues or [])}"
            )


class EnrichedPSM(PSMWithModifications):
    """PSM with modifications and downloaded spectrum data."""

    # Spectrum data arrays
    mz_array: list[float]
    intensity_array: list[float]

    def model_post_init(self, __context: Any) -> None:
        """Validate both modification and spectrum arrays."""
        # Call parent validation
        super().model_post_init(__context)

        # Validate spectrum arrays match
        if len(self.mz_array) != len(self.intensity_array):
            raise ValueError(
                f"Spectrum array length mismatch: "
                f"mz_array={len(self.mz_array)}, "
                f"intensity_array={len(self.intensity_array)}"
            )

