"""Seed database with minimal sample data for development."""

from datetime import date, datetime

from sqlalchemy.engine.base import Engine
from sqlmodel import Session

from usigrabber.db.schema import (
    IndexType,
    ModifiedPeptide,
    MzidFile,
    PeptideEvidence,
    PeptideSpectrumMatch,
    Project,
    ProjectCountry,
    ProjectKeyword,
    ProjectTag,
    PSMPeptideEvidence,
    Reference,
)


def seed_minimal_data(engine: Engine) -> None:
    """
    Seed database with minimal sample PRIDE project data.

    Creates:
    - 2 projects with relationships
    - 2 references
    - CV parameters (instruments, organisms)
    - Keywords, tags, countries
    """

    with Session(engine) as session:
        # 1. Create Projects
        project1 = Project(
            accession="PXD000001",
            title="Proteomics Analysis of Human Cancer Cell Lines",
            projectDescription="Comprehensive proteomics study of human cancer ...",
            sampleProcessingProtocol="Cells lysed, proteins extracted and ...",
            dataProcessingProtocol="Data analyzed with MaxQuant and Perseus.",
            submissionType="COMPLETE",
            submissionDate=date(2023, 1, 15),
            publicationDate=date(2023, 6, 1),
            totalFileDownloads=523,
            fully_processed=True,
        )

        project2 = Project(
            accession="PXD000002",
            title="Mouse Brain Development Proteome",
            projectDescription="Temporal proteomics analysis of mouse brain ...",
            submissionType="PARTIAL",
            submissionDate=date(2023, 3, 10),
            publicationDate=date(2023, 8, 15),
            totalFileDownloads=187,
            fully_processed=False,
        )

        session.add_all([project1, project2])
        session.flush()

        # 3. Create References
        references = [
            Reference(
                project_accession="PXD000001",
                referenceLine="Smith J, et al. Proteomics of cancer cells. Nature.",
                pubmedID=12345678,
                doi="10.1038/nature.2023.001",
            ),
            Reference(
                project_accession="PXD000002",
                referenceLine="Doe J, et al. Brain development proteome. Cell. 2023",
                pubmedID=87654321,
                doi="10.1016/cell.2023.002",
            ),
        ]
        session.add_all(references)

        # 4. Create Keywords
        keywords = [
            ProjectKeyword(project_accession="PXD000001", keyword="cancer"),
            ProjectKeyword(project_accession="PXD000001", keyword="proteomics"),
            ProjectKeyword(project_accession="PXD000001", keyword="human"),
            ProjectKeyword(project_accession="PXD000002", keyword="brain"),
            ProjectKeyword(project_accession="PXD000002", keyword="development"),
            ProjectKeyword(project_accession="PXD000002", keyword="mouse"),
        ]
        session.add_all(keywords)

        # 5. Create Tags
        tags = [
            ProjectTag(project_accession="PXD000001", tag="Biological"),
            ProjectTag(project_accession="PXD000001", tag="Medical"),
            ProjectTag(project_accession="PXD000002", tag="Biological"),
        ]
        session.add_all(tags)

        # 6. Create Countries
        countries = [
            ProjectCountry(project_accession="PXD000001", country="United States"),
            ProjectCountry(project_accession="PXD000001", country="United Kingdom"),
            ProjectCountry(project_accession="PXD000002", country="United Kingdom"),
            ProjectCountry(project_accession="PXD000002", country="Spain"),
        ]
        session.add_all(countries)

        session.commit()

        # =====================================================================
        # mzID Mock Data - Very minimal for testing
        # =====================================================================

        # 1. Create a mock mzID file
        mzid_file = MzidFile(
            project_accession="PXD000001",
            file_name="mock_data.mzid",
            file_path="/mock/path/mock_data.mzid",
            software_name="MS-GF+",
            software_version="v2023.01",
            threshold_type="FDR",
            threshold_value=0.01,
            creation_date=datetime(2023, 1, 20),
        )
        session.add(mzid_file)

        # 2. Create peptides
        peptide1 = ModifiedPeptide(id="PEPTIDER", peptide_sequence="PEPTIDER")
        peptide2 = ModifiedPeptide(id="EXAMPLE", peptide_sequence="EXAMPLE")
        peptide3 = ModifiedPeptide(id="SAMPLEPEP", peptide_sequence="SAMPLEPEP")
        session.add_all([peptide1, peptide2, peptide3])

        # 3. Create PSMs
        psm1 = PeptideSpectrumMatch(
            project_accession="PXD000001",
            mzid_file_id=mzid_file.id,
            modified_peptide_id=peptide1.id,
            spectrum_id="scan=1234",
            charge_state=2,
            experimental_mz=450.234,
            calculated_mz=450.235,
            score_values={"MS-GF:SpecEValue": 1.2e-10, "MS-GF:QValue": 0.001},
            rank=1,
            pass_threshold=True,
            index_type=IndexType.scan,
            index_number=1234,
            ms_run="mock_data_file",
        )
        psm2 = PeptideSpectrumMatch(
            project_accession="PXD000001",
            mzid_file_id=mzid_file.id,
            modified_peptide_id=peptide2.id,
            spectrum_id="scan=5678",
            charge_state=3,
            experimental_mz=325.678,
            calculated_mz=325.679,
            score_values={"MS-GF:SpecEValue": 5.6e-8, "MS-GF:QValue": 0.005},
            rank=1,
            pass_threshold=True,
            index_type=IndexType.scan,
            index_number=5678,
            ms_run="mock_data_file",
        )
        psm3 = PeptideSpectrumMatch(
            project_accession="PXD000001",
            mzid_file_id=mzid_file.id,
            modified_peptide_id=peptide3.id,
            spectrum_id="scan=9012",
            charge_state=2,
            experimental_mz=380.123,
            calculated_mz=380.124,
            score_values={"MS-GF:SpecEValue": 2.3e-6, "MS-GF:QValue": 0.008},
            rank=1,
            pass_threshold=True,
            index_type=IndexType.scan,
            index_number=9012,
            ms_run="mock_data_file",
        )
        session.add_all([psm1, psm2, psm3])

        # 4. Create peptide evidence (protein mappings)
        evidence1 = PeptideEvidence(
            protein_accession="P12345",
            start_position=45,
            end_position=52,
            pre_residue="K",
            post_residue="A",
            is_decoy=False,
        )
        evidence2 = PeptideEvidence(
            protein_accession="Q67890",
            start_position=120,
            end_position=126,
            pre_residue="R",
            post_residue="G",
            is_decoy=False,
        )
        evidence3 = PeptideEvidence(
            protein_accession="A11111",
            start_position=78,
            end_position=84,
            pre_residue="K",
            post_residue="L",
            is_decoy=False,
        )
        session.add_all([evidence1, evidence2, evidence3])

        # 5. Link PSMs to protein evidence through junction table
        psm_evidence1 = PSMPeptideEvidence(psm_id=psm1.id, peptide_evidence_id=evidence1.id)
        psm_evidence2 = PSMPeptideEvidence(psm_id=psm2.id, peptide_evidence_id=evidence2.id)
        psm_evidence3 = PSMPeptideEvidence(psm_id=psm3.id, peptide_evidence_id=evidence3.id)
        session.add_all([psm_evidence1, psm_evidence2, psm_evidence3])

        session.commit()


if __name__ == "__main__":
    from usigrabber.db import create_db_and_tables, load_db_engine

    print("Creating database and seeding with sample data...")
    engine = load_db_engine()
    create_db_and_tables(engine)
    seed_minimal_data(engine)
    print("✅ Database seeded successfully!")
