from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def project1_evidence_df():
    return pd.read_csv(Path("tests/txt_zip/fixtures/project1/evidence.txt"), sep="\t")


@pytest.fixture
def project1_peptides_df():
    return pd.read_csv(Path("tests/txt_zip/fixtures/project1/peptides.txt"), sep="\t")


@pytest.fixture
def project1_summary_df():
    return pd.read_csv(Path("tests/txt_zip/fixtures/project1/summary.txt"), sep="\t")


@pytest.fixture
def project2_evidence_df():
    return pd.read_csv(Path("tests/txt_zip/fixtures/project2/evidence.txt"), sep="\t")


@pytest.fixture
def project2_peptides_df():
    return pd.read_csv(Path("tests/txt_zip/fixtures/project2/peptides.txt"), sep="\t")


@pytest.fixture
def project2_summary_df():
    return pd.read_csv(Path("tests/txt_zip/fixtures/project2/summary.txt"), sep="\t")
