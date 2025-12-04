import uuid

# Namespace UUIDs for generating deterministic UUIDs
# These ensure the same properties always generate the same UUID
MODIFICATION_NAMESPACE = uuid.UUID("a8f3c5e7-9b2d-4f1a-8c6e-3d7b5a9f2e4c")
PEPTIDE_NAMESPACE = uuid.UUID("b9e4d6f8-0c3a-5e2b-9d7f-4a8c6e1b3d5a")


def generate_deterministic_modification_uuid(
    unimod_id: int | None,
    name: str | None,
    position: int | None,
    modified_residue: str | None,
) -> uuid.UUID:
    """
    Generate a deterministic UUID for a modification based on its properties.

    Uses UUID v5 (SHA-1 hash-based) to ensure identical modifications always
    receive identical UUIDs across all files and parsing runs.

    Args:
        unimod_id: UNIMOD identifier (e.g., 35 for Oxidation)
        name: Modification name (e.g., "Oxidation")
        position: Position in the peptide sequence
        modified_residue: Residue(s) being modified (e.g., "M")

    Returns:
        Deterministic UUID v5 based on the modification properties
    """
    # Create a deterministic string representation of the modification
    # Format: "unimod:{id}|name:{name}|pos:{position}|residue:{residue}"
    parts = [
        f"unimod:{unimod_id if unimod_id is not None else 'null'}",
        f"name:{name if name is not None else 'null'}",
        f"pos:{position if position is not None else 'null'}",
        f"residue:{modified_residue if modified_residue is not None else 'null'}",
    ]
    mod_string = "|".join(parts)

    # Generate UUID v5 using the namespace and modification string
    return uuid.uuid5(MODIFICATION_NAMESPACE, mod_string)


def generate_deterministic_peptide_uuid(
    sequence: str,
    modification_signature: str,
) -> uuid.UUID:
    """
    Generate a deterministic UUID for a modified peptide based on its properties.

    Uses UUID v5 (SHA-1 hash-based) to ensure identical modified peptides always
    receive identical UUIDs across all files and parsing runs.

    Args:
        sequence: Peptide amino acid sequence (e.g., "PEPTIDESEQ")
        modification_signature: Sorted modification signature (e.g., "unimod35@5_unimod4@10")

    Returns:
        Deterministic UUID v5 based on the peptide sequence and modifications
    """
    # Create a deterministic string representation of the modified peptide
    # Format: "seq:{sequence}|mods:{signature}"
    peptide_string = f"seq:{sequence}|mods:{modification_signature or 'none'}"

    # Generate UUID v5 using the namespace and peptide string
    return uuid.uuid5(PEPTIDE_NAMESPACE, peptide_string)
