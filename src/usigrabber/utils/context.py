from contextvars import ContextVar

context_project_accession: ContextVar[str | None] = ContextVar("project_accession")
context_file_id: ContextVar[str | None] = ContextVar("file_id")
