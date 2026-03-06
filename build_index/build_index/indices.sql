-- 1. CRITICAL: Increase memory for sorting. 
-- If you have 16GB+ RAM, '4GB' is safe. If less, try '1GB'.
-- This prevents the DB from writing temp files to disk (which kills speed).
SET maintenance_work_mem = '16GB';

-- 2. CRITICAL: Use multiple CPU cores.
-- If you have a 4+ core CPU, this allows Postgres to sort in parallel.
SET max_parallel_maintenance_workers = 8; 

-- 3. Create Indices (Standard/Blocking Mode)

-- PSM Foreign Keys
CREATE INDEX IF NOT EXISTS idx_psm_project_accession 
ON peptide_spectrum_matches (project_accession);

CREATE INDEX IF NOT EXISTS idx_psm_modified_peptide_id 
ON peptide_spectrum_matches (modified_peptide_id);

-- CREATE INDEX IF NOT EXISTS idx_psm_mzid_file_id 
-- ON peptide_spectrum_matches (mzid_file_id);

-- Junction Table Indices
-- not required because its covered by PK
-- CREATE INDEX IF NOT EXISTS idx_ppe_psm_id 
-- ON psm_peptide_evidence (psm_id);

CREATE INDEX IF NOT EXISTS idx_ppe_peptide_evidence_id 
ON psm_peptide_evidence (peptide_evidence_id);
