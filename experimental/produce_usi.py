import urllib.error

from pyteomics import usi
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from usigrabber.db import load_db_engine
from usigrabber.db.schema import PeptideSpectrumMatch, Project


def get_spectrum(usi_str: str):
    try:
        return usi.proxi(usi_str, backend="pride")
    except urllib.error.HTTPError as e:
        if e.code == 500:
            raise ValueError(f"Spectrum not found for USI: {usi_str}") from e
        raise e


def main():
    engine = load_db_engine()
    with Session(engine) as session:
        # get a random PSM from the database
        # exclude known broken projects
        statement = (
            select(PeptideSpectrumMatch)
            .where(PeptideSpectrumMatch.project.has(Project.accession != "PXD005152"))  # type: ignore
            .order_by(func.random())
            .limit(1)
        )
        psm = session.execute(statement).scalar_one_or_none()
        if (
            not psm
            or not psm.project
            or not psm.ms_run
            or not psm.index_type
            or not psm.index_number
            or not psm.modified_peptide
            or not psm.charge_state
        ):
            print("Insufficient data to generate USI.")
            return
        usi_str = (
            f"mzspec:{psm.project.accession}:{psm.ms_run}:"
            f"{psm.index_type.value}:{psm.index_number}:{psm.modified_peptide.peptide_sequence}/{psm.charge_state}"
        )

        print("Retrieving spectrum for USI:", usi_str)
        spectrum = get_spectrum(usi_str)
        print(spectrum)


if __name__ == "__main__":
    main()
