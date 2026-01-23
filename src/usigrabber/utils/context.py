from contextvars import ContextVar

context_project_accession: ContextVar[str] = ContextVar("project_accession")
context_file_id: ContextVar[str] = ContextVar("file_id")
