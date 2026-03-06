--
-- PostgreSQL database dump
--

\restrict 9aqusQg3fc17HM4k8VLV0hF3OVdd0d0D1YVwa5qWBFI4KrrCrgH91MZEUngZy0G

-- Dumped from database version 18.1 (Debian 18.1-1.pgdg13+2)
-- Dumped by pg_dump version 18.1 (Debian 18.1-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

--
-- Name: cv_params cv_params_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.cv_params
    ADD CONSTRAINT cv_params_pkey PRIMARY KEY (id);


--
-- Name: downloaded_files downloaded_files_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.downloaded_files
    ADD CONSTRAINT downloaded_files_pkey PRIMARY KEY (id);


--
-- Name: imported_files imported_files_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.imported_files
    ADD CONSTRAINT imported_files_pkey PRIMARY KEY (id);


--
-- Name: modifications modifications_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.modifications
    ADD CONSTRAINT modifications_pkey PRIMARY KEY (id);


--
-- Name: modified_peptide_modification_junction modified_peptide_modification_junction_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.modified_peptide_modification_junction
    ADD CONSTRAINT modified_peptide_modification_junction_pkey PRIMARY KEY (modified_peptide_id, modification_id);


--
-- Name: modified_peptides modified_peptides_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.modified_peptides
    ADD CONSTRAINT modified_peptides_pkey PRIMARY KEY (id);


--
-- Name: mzid_files mzid_files_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.mzid_files
    ADD CONSTRAINT mzid_files_pkey PRIMARY KEY (id);


--
-- Name: peptide_evidence peptide_evidence_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.peptide_evidence
    ADD CONSTRAINT peptide_evidence_pkey PRIMARY KEY (id);


--
-- Name: peptide_spectrum_matches peptide_spectrum_matches_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.peptide_spectrum_matches
    ADD CONSTRAINT peptide_spectrum_matches_pkey PRIMARY KEY (id);


--
-- Name: project_affiliations project_affiliations_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_affiliations
    ADD CONSTRAINT project_affiliations_pkey PRIMARY KEY (id);


--
-- Name: project_countries project_countries_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_countries
    ADD CONSTRAINT project_countries_pkey PRIMARY KEY (id);


--
-- Name: project_cv_params project_cv_params_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_cv_params
    ADD CONSTRAINT project_cv_params_pkey PRIMARY KEY (cv_param_id, project_accession);


--
-- Name: project_keywords project_keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_keywords
    ADD CONSTRAINT project_keywords_pkey PRIMARY KEY (id);


--
-- Name: project_other_omics_links project_other_omics_links_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_other_omics_links
    ADD CONSTRAINT project_other_omics_links_pkey PRIMARY KEY (id);


--
-- Name: project_tags project_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_tags
    ADD CONSTRAINT project_tags_pkey PRIMARY KEY (id);


--
-- Name: projects projects_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (accession);


--
-- Name: psm_peptide_evidence psm_peptide_evidence_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.psm_peptide_evidence
    ADD CONSTRAINT psm_peptide_evidence_pkey PRIMARY KEY (id);


--
-- Name: references references_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public."references"
    ADD CONSTRAINT references_pkey PRIMARY KEY (id);


--
-- Name: search_modifications search_modifications_pkey; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.search_modifications
    ADD CONSTRAINT search_modifications_pkey PRIMARY KEY (id);


--
-- Name: modifications uix_mod_unique; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.modifications
    ADD CONSTRAINT uix_mod_unique UNIQUE (unimod_id, name, location, modified_residue);


--
-- Name: downloaded_files unique_download_file_constraint; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.downloaded_files
    ADD CONSTRAINT unique_download_file_constraint UNIQUE (file_name, project_accession);


--
-- Name: imported_files unique_file_constraint; Type: CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.imported_files
    ADD CONSTRAINT unique_file_constraint UNIQUE (file_id, project_accession);


--
-- Name: imported_files imported_files_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.imported_files
    ADD CONSTRAINT imported_files_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: modified_peptide_modification_junction modified_peptide_modification_junction_modification_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.modified_peptide_modification_junction
    ADD CONSTRAINT modified_peptide_modification_junction_modification_id_fkey FOREIGN KEY (modification_id) REFERENCES public.modifications(id);


--
-- Name: modified_peptide_modification_junction modified_peptide_modification_junction_modified_peptide_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.modified_peptide_modification_junction
    ADD CONSTRAINT modified_peptide_modification_junction_modified_peptide_id_fkey FOREIGN KEY (modified_peptide_id) REFERENCES public.modified_peptides(id);


--
-- Name: mzid_files mzid_files_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.mzid_files
    ADD CONSTRAINT mzid_files_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: peptide_spectrum_matches peptide_spectrum_matches_modified_peptide_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.peptide_spectrum_matches
    ADD CONSTRAINT peptide_spectrum_matches_modified_peptide_id_fkey FOREIGN KEY (modified_peptide_id) REFERENCES public.modified_peptides(id);


--
-- Name: peptide_spectrum_matches peptide_spectrum_matches_mzid_file_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.peptide_spectrum_matches
    ADD CONSTRAINT peptide_spectrum_matches_mzid_file_id_fkey FOREIGN KEY (mzid_file_id) REFERENCES public.mzid_files(id);


--
-- Name: peptide_spectrum_matches peptide_spectrum_matches_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.peptide_spectrum_matches
    ADD CONSTRAINT peptide_spectrum_matches_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: project_affiliations project_affiliations_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_affiliations
    ADD CONSTRAINT project_affiliations_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: project_countries project_countries_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_countries
    ADD CONSTRAINT project_countries_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: project_cv_params project_cv_params_cv_param_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_cv_params
    ADD CONSTRAINT project_cv_params_cv_param_id_fkey FOREIGN KEY (cv_param_id) REFERENCES public.cv_params(id);


--
-- Name: project_cv_params project_cv_params_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_cv_params
    ADD CONSTRAINT project_cv_params_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: project_keywords project_keywords_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_keywords
    ADD CONSTRAINT project_keywords_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: project_other_omics_links project_other_omics_links_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_other_omics_links
    ADD CONSTRAINT project_other_omics_links_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: project_tags project_tags_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_tags
    ADD CONSTRAINT project_tags_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: psm_peptide_evidence psm_peptide_evidence_peptide_evidence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.psm_peptide_evidence
    ADD CONSTRAINT psm_peptide_evidence_peptide_evidence_id_fkey FOREIGN KEY (peptide_evidence_id) REFERENCES public.peptide_evidence(id);


--
-- Name: psm_peptide_evidence psm_peptide_evidence_psm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.psm_peptide_evidence
    ADD CONSTRAINT psm_peptide_evidence_psm_id_fkey FOREIGN KEY (psm_id) REFERENCES public.peptide_spectrum_matches(id);


--
-- Name: references references_project_accession_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public."references"
    ADD CONSTRAINT references_project_accession_fkey FOREIGN KEY (project_accession) REFERENCES public.projects(accession);


--
-- Name: search_modifications search_modifications_psm_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.search_modifications
    ADD CONSTRAINT search_modifications_psm_id_fkey FOREIGN KEY (psm_id) REFERENCES public.peptide_spectrum_matches(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 9aqusQg3fc17HM4k8VLV0hF3OVdd0d0D1YVwa5qWBFI4KrrCrgH91MZEUngZy0G
