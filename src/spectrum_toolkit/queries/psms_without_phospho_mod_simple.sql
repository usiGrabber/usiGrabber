SELECT 
    psm.id AS psm_id,
    psm.project_accession,
    psm.charge_state,
	psm.ms_run,
	psm.index_number,
	psm.index_type,
    mp.peptide_sequence
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
