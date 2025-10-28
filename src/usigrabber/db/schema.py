from sqlmodel import Field, SQLModel, create_engine


class Project(SQLModel, table=True):
	accession: str = Field(primary_key=True)
	title: str
	submission_type: str
	description: str
	sample_processing_protocol: str
	data_processing_protocol: str


sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

engine = create_engine(sqlite_url, echo=True)


def create_db_and_tables():
	SQLModel.metadata.create_all(engine)


if __name__ == "__main__":
	create_db_and_tables()
