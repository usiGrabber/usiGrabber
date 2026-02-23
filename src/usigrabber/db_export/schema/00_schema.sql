--
-- PostgreSQL database dump
--

\restrict d9eVtsDgFzwcjcfCyZumIuEbkxPfeDpVasSJhkCAMfKOhFba86kZexRYjApmExr

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

--
-- Name: indextype; Type: TYPE; Schema: public; Owner: user
--

CREATE TYPE public.indextype AS ENUM (
    'scan',
    'index',
    'nativeId',
    'trace'
);


ALTER TYPE public.indextype OWNER TO "user";

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: cv_params; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.cv_params (
    id integer NOT NULL,
    accession character varying NOT NULL,
    value character varying
);


ALTER TABLE public.cv_params OWNER TO "user";

--
-- Name: cv_params_id_seq; Type: SEQUENCE; Schema: public; Owner: user
--

CREATE SEQUENCE public.cv_params_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.cv_params_id_seq OWNER TO "user";

--
-- Name: cv_params_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: user
--

ALTER SEQUENCE public.cv_params_id_seq OWNED BY public.cv_params.id;


--
-- Name: downloaded_files; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.downloaded_files (
    id integer NOT NULL,
    project_accession character varying NOT NULL,
    file_name character varying NOT NULL,
    file_size integer,
    checksum character(32),
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone,
    is_successful boolean,
    error_message character varying,
    traceback character varying,
    job_id character varying NOT NULL,
    worker_pid integer NOT NULL
);


ALTER TABLE public.downloaded_files OWNER TO "user";

--
-- Name: downloaded_files_id_seq; Type: SEQUENCE; Schema: public; Owner: user
--

CREATE SEQUENCE public.downloaded_files_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.downloaded_files_id_seq OWNER TO "user";

--
-- Name: downloaded_files_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: user
--

ALTER SEQUENCE public.downloaded_files_id_seq OWNED BY public.downloaded_files.id;


--
-- Name: imported_files; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.imported_files (
    id integer NOT NULL,
    project_accession character varying NOT NULL,
    file_id character varying NOT NULL,
    format character varying NOT NULL,
    psm_count integer,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone,
    is_processed_successfully boolean,
    error_message character varying,
    traceback character varying,
    worker_pid integer NOT NULL,
    job_id character varying NOT NULL,
    checksum character(32) NOT NULL
);


ALTER TABLE public.imported_files OWNER TO "user";

--
-- Name: imported_files_id_seq; Type: SEQUENCE; Schema: public; Owner: user
--

CREATE SEQUENCE public.imported_files_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.imported_files_id_seq OWNER TO "user";

--
-- Name: imported_files_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: user
--

ALTER SEQUENCE public.imported_files_id_seq OWNED BY public.imported_files.id;


--
-- Name: modifications; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.modifications (
    id uuid NOT NULL,
    unimod_id integer,
    name character varying,
    location integer,
    modified_residue character varying,
    CONSTRAINT chk_mod_name_or_unimodid_null CHECK (((unimod_id IS NULL) OR (name IS NULL)))
);


ALTER TABLE public.modifications OWNER TO "user";

--
-- Name: modified_peptide_modification_junction; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.modified_peptide_modification_junction (
    modified_peptide_id uuid CONSTRAINT modified_peptide_modification_junc_modified_peptide_id_not_null NOT NULL,
    modification_id uuid NOT NULL
);


ALTER TABLE public.modified_peptide_modification_junction OWNER TO "user";

--
-- Name: modified_peptides; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.modified_peptides (
    id uuid NOT NULL,
    peptide_sequence character varying NOT NULL
);


ALTER TABLE public.modified_peptides OWNER TO "user";

--
-- Name: mzid_files; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.mzid_files (
    id uuid NOT NULL,
    project_accession character varying NOT NULL,
    file_name character varying NOT NULL,
    file_path character varying,
    software_name character varying,
    software_version character varying,
    search_database_name character varying,
    protocol_parameters json,
    threshold_type character varying,
    threshold_value double precision,
    creation_date timestamp without time zone
);


ALTER TABLE public.mzid_files OWNER TO "user";

--
-- Name: peptide_evidence; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.peptide_evidence (
    id uuid NOT NULL,
    protein_accession character varying,
    is_decoy boolean,
    start_position integer,
    end_position integer,
    pre_residue character varying(1),
    post_residue character varying(1)
);


ALTER TABLE public.peptide_evidence OWNER TO "user";

--
-- Name: peptide_spectrum_matches; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.peptide_spectrum_matches (
    id uuid NOT NULL,
    project_accession character varying NOT NULL,
    mzid_file_id uuid,
    modified_peptide_id uuid NOT NULL,
    spectrum_id character varying,
    charge_state integer,
    experimental_mz double precision,
    calculated_mz double precision,
    score_values json,
    rank integer,
    pass_threshold boolean,
    index_type public.indextype,
    index_number integer,
    ms_run character varying,
    ms_run_ext character varying,
    is_usi_validated boolean
);


ALTER TABLE public.peptide_spectrum_matches OWNER TO "user";

--
-- Name: project_affiliations; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.project_affiliations (
    id integer NOT NULL,
    project_accession character varying NOT NULL,
    affiliation character varying NOT NULL
);


ALTER TABLE public.project_affiliations OWNER TO "user";

--
-- Name: project_affiliations_id_seq; Type: SEQUENCE; Schema: public; Owner: user
--

CREATE SEQUENCE public.project_affiliations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.project_affiliations_id_seq OWNER TO "user";

--
-- Name: project_affiliations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: user
--

ALTER SEQUENCE public.project_affiliations_id_seq OWNED BY public.project_affiliations.id;


--
-- Name: project_countries; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.project_countries (
    id integer NOT NULL,
    project_accession character varying NOT NULL,
    country character varying NOT NULL
);


ALTER TABLE public.project_countries OWNER TO "user";

--
-- Name: project_countries_id_seq; Type: SEQUENCE; Schema: public; Owner: user
--

CREATE SEQUENCE public.project_countries_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.project_countries_id_seq OWNER TO "user";

--
-- Name: project_countries_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: user
--

ALTER SEQUENCE public.project_countries_id_seq OWNED BY public.project_countries.id;


--
-- Name: project_cv_params; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.project_cv_params (
    cv_param_id integer NOT NULL,
    project_accession character varying NOT NULL
);


ALTER TABLE public.project_cv_params OWNER TO "user";

--
-- Name: project_keywords; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.project_keywords (
    id integer NOT NULL,
    project_accession character varying NOT NULL,
    keyword character varying NOT NULL
);


ALTER TABLE public.project_keywords OWNER TO "user";

--
-- Name: project_keywords_id_seq; Type: SEQUENCE; Schema: public; Owner: user
--

CREATE SEQUENCE public.project_keywords_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.project_keywords_id_seq OWNER TO "user";

--
-- Name: project_keywords_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: user
--

ALTER SEQUENCE public.project_keywords_id_seq OWNED BY public.project_keywords.id;


--
-- Name: project_other_omics_links; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.project_other_omics_links (
    id integer NOT NULL,
    project_accession character varying NOT NULL,
    link character varying NOT NULL
);


ALTER TABLE public.project_other_omics_links OWNER TO "user";

--
-- Name: project_other_omics_links_id_seq; Type: SEQUENCE; Schema: public; Owner: user
--

CREATE SEQUENCE public.project_other_omics_links_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.project_other_omics_links_id_seq OWNER TO "user";

--
-- Name: project_other_omics_links_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: user
--

ALTER SEQUENCE public.project_other_omics_links_id_seq OWNED BY public.project_other_omics_links.id;


--
-- Name: project_tags; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.project_tags (
    id integer NOT NULL,
    project_accession character varying NOT NULL,
    tag character varying NOT NULL
);


ALTER TABLE public.project_tags OWNER TO "user";

--
-- Name: project_tags_id_seq; Type: SEQUENCE; Schema: public; Owner: user
--

CREATE SEQUENCE public.project_tags_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.project_tags_id_seq OWNER TO "user";

--
-- Name: project_tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: user
--

ALTER SEQUENCE public.project_tags_id_seq OWNED BY public.project_tags.id;


--
-- Name: projects; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.projects (
    accession character varying NOT NULL,
    title character varying NOT NULL,
    project_description character varying,
    sample_processing_protocol character varying,
    data_processing_protocol character varying,
    doi character varying,
    submission_type character varying NOT NULL,
    license character varying,
    submission_date date,
    publication_date date,
    total_file_downloads integer NOT NULL,
    error_message character varying,
    traceback character varying,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone,
    job_id character varying NOT NULL,
    worker_pid integer NOT NULL,
    sample_attributes json,
    additional_attributes json
);


ALTER TABLE public.projects OWNER TO "user";

--
-- Name: psm_peptide_evidence; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.psm_peptide_evidence (
    id uuid NOT NULL,
    psm_id uuid NOT NULL,
    peptide_evidence_id uuid NOT NULL
);


ALTER TABLE public.psm_peptide_evidence OWNER TO "user";

--
-- Name: references; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public."references" (
    id integer NOT NULL,
    project_accession character varying NOT NULL,
    reference_line character varying,
    pubmed_id integer,
    doi character varying
);


ALTER TABLE public."references" OWNER TO "user";

--
-- Name: references_id_seq; Type: SEQUENCE; Schema: public; Owner: user
--

CREATE SEQUENCE public.references_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.references_id_seq OWNER TO "user";

--
-- Name: references_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: user
--

ALTER SEQUENCE public.references_id_seq OWNED BY public."references".id;


--
-- Name: search_modifications; Type: TABLE; Schema: public; Owner: user
--

CREATE TABLE public.search_modifications (
    id uuid NOT NULL,
    psm_id uuid NOT NULL,
    unimod_id integer NOT NULL
);


ALTER TABLE public.search_modifications OWNER TO "user";

--
-- Name: cv_params id; Type: DEFAULT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.cv_params ALTER COLUMN id SET DEFAULT nextval('public.cv_params_id_seq'::regclass);


--
-- Name: downloaded_files id; Type: DEFAULT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.downloaded_files ALTER COLUMN id SET DEFAULT nextval('public.downloaded_files_id_seq'::regclass);


--
-- Name: imported_files id; Type: DEFAULT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.imported_files ALTER COLUMN id SET DEFAULT nextval('public.imported_files_id_seq'::regclass);


--
-- Name: project_affiliations id; Type: DEFAULT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_affiliations ALTER COLUMN id SET DEFAULT nextval('public.project_affiliations_id_seq'::regclass);


--
-- Name: project_countries id; Type: DEFAULT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_countries ALTER COLUMN id SET DEFAULT nextval('public.project_countries_id_seq'::regclass);


--
-- Name: project_keywords id; Type: DEFAULT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_keywords ALTER COLUMN id SET DEFAULT nextval('public.project_keywords_id_seq'::regclass);


--
-- Name: project_other_omics_links id; Type: DEFAULT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_other_omics_links ALTER COLUMN id SET DEFAULT nextval('public.project_other_omics_links_id_seq'::regclass);


--
-- Name: project_tags id; Type: DEFAULT; Schema: public; Owner: user
--

ALTER TABLE ONLY public.project_tags ALTER COLUMN id SET DEFAULT nextval('public.project_tags_id_seq'::regclass);


--
-- Name: references id; Type: DEFAULT; Schema: public; Owner: user
--

ALTER TABLE ONLY public."references" ALTER COLUMN id SET DEFAULT nextval('public.references_id_seq'::regclass);


--
-- PostgreSQL database dump complete
--

\unrestrict d9eVtsDgFzwcjcfCyZumIuEbkxPfeDpVasSJhkCAMfKOhFba86kZexRYjApmExr
