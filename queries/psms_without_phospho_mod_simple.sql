SELECT 
    psm.id AS psm_id,
    psm.project_accession,
    psm.spectrum_id,
    psm.charge_state,
    psm.experimental_mz,
    psm.calculated_mz,
    psm.pass_threshold,
    psm.rank,
	psm.ms_run,
	psm.index_number,
	psm.index_type,
    mp.peptide_sequence,
    mp.id AS modified_peptide_id,
    m.unimod_id,
    m.location,
    m.modified_residue
FROM peptide_spectrum_matches psm
INNER JOIN modified_peptides mp 
    ON psm.modified_peptide_id = mp.id
INNER JOIN modified_peptide_modification_junction mpmj
    ON mp.id = mpmj.modified_peptide_id
INNER JOIN modifications m 
    ON mpmj.modification_id = m.id
INNER JOIN search_modifications sm
    ON psm.id = sm.psm_id
    AND sm.unimod_id = 21
WHERE m.unimod_id != 21;

