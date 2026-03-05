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
LIMIT 100000;
