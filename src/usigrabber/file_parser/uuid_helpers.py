import uuid
from typing import Any

# Namespace UUIDs for generating deterministic UUIDs
# These ensure the same properties always generate the same UUID
MODIFICATION_NAMESPACE = uuid.UUID("a8f3c5e7-9b2d-4f1a-8c6e-3d7b5a9f2e4c")
PEPTIDE_NAMESPACE = uuid.UUID("b9e4d6f8-0c3a-5e2b-9d7f-4a8c6e1b3d5a")


def generate_deterministic_modification_uuid(
    unimod_id: int | None,
    name: str | None,
    location: int | None,
    modified_residue: str | None,
) -> uuid.UUID:
    """
    Generate a deterministic UUID for a modification based on its properties.

    Uses UUID v5 (SHA-1 hash-based) to ensure identical modifications always
    receive identical UUIDs across all files and parsing runs.

    Args:
        unimod_id: UNIMOD identifier (e.g., 35 for Oxidation)
        name: Modification name (e.g., "Oxidation")
        location: Location in the peptide sequence
        modified_residue: Residue(s) being modified (e.g., "M")

    Returns:
        Deterministic UUID v5 based on the modification properties
    """
    # Create a deterministic string representation of the modification
    # Format: "unimod:{id}|name:{name}|pos:{position}|residue:{residue}"
    parts = [
        f"unimod:{unimod_id if unimod_id is not None else 'null'}",
        f"name:{name if name is not None else 'null'}",
        f"loc:{location if location is not None else 'null'}",
        f"residue:{modified_residue if modified_residue is not None else 'null'}",
    ]
    mod_string = "|".join(parts)

    # Generate UUID v5 using the namespace and modification string
    return uuid.uuid5(MODIFICATION_NAMESPACE, mod_string)


def generate_deterministic_peptide_uuid(
    sequence: str,
    parsed_mods: list[dict[str, Any]],
) -> uuid.UUID:
    """
    Generate a deterministic UUID for a modified peptide based on its properties.

    Uses UUID v5 (SHA-1 hash-based) to ensure identical modified peptides always
    receive identical UUIDs across all files and parsing runs.

    Args:
        sequence: Peptide amino acid sequence (e.g., "PEPTIDESEQ")
        parsed_mods: List of parsed modification dicts with id, unimod_id, name, location,

    Returns:
        Deterministic UUID v5 based on the peptide sequence and modifications
    """

    mod_parts = []
    for mod in parsed_mods:
        unimod_id = mod["unimod_id"]
        name = mod["name"]
        location = mod["location"]
        modified_residue = mod["modified_residue"]

        # Use unimod ID if available, otherwise use name
        mod_identifier = f"unimod{unimod_id}" if unimod_id else (name or "unknown")
        # Clean identifier to remove special characters
        mod_identifier = mod_identifier.replace(":", "_").replace(" ", "_")

        loc_str = str(location) if location is not None else "unk"
        residue_str = modified_residue if modified_residue else "X"
        mod_parts.append(f"{mod_identifier}@{loc_str}@{residue_str}")

    # Sort by location to ensure deterministic IDs
    mod_parts.sort()
    modification_signature: str = "_".join(mod_parts)

    # Create a deterministic string representation of the modified peptide
    # Format: "seq:{sequence}|mods:{signature}"
    peptide_string = f"seq:{sequence}|mods:{modification_signature or 'none'}"

    # Generate UUID v5 using the namespace and peptide string
    return uuid.uuid5(PEPTIDE_NAMESPACE, peptide_string)
