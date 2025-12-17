import os
from pathlib import Path

from usigrabber.db import load_db_engine
from usigrabber.file_parser import import_file

mzid_dir = "./data/mzids/"

engine = load_db_engine()

for filename in os.listdir(mzid_dir):
    if filename.endswith(".mzid"):
        filepath = os.path.join(mzid_dir, filename)
        print(f"Importing: {filepath}")
        result = import_file(engine, Path(filepath), ".mzid", "TEST_PROJECT")
        # Do something with result if needed
