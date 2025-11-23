import asyncio
import os
from pathlib import Path

from usigrabber.backends import pride
from usigrabber.backends.base import Files
from usigrabber.utils.file import download_ftp


async def main():
    FILETYPE_WHITELIST = {".mzid", "", ".txt"}
    ARCHIVE_TYPES = {".zip", ".gz", ".tar", ".rar", ".7z"}

    projects = [
        "PXD068559",
        "PXD066746",
        "PXD066470",
        "PXD070409",
        "PXD065854",
        "PXD066156",
        "PXD066215",
        "PXD066365",
        "PXD068739",
    ]

    print(os.path.dirname("C:/Users/nils.py"))

    for project in projects:
        files: Files = pride.PrideBackend.get_files_for_project(project)
        print(f"Project: {project}, all Files: {files}")
        for file in files["search"]:
            file_url = file["filepath"]
            filename = os.path.basename(file_url)

            # find actual file extension, without archives
            file_base, file_ext = os.path.splitext(filename)
            while file_ext in ARCHIVE_TYPES:
                file_base, file_ext = os.path.splitext(file_base)

            if (file_ext not in FILETYPE_WHITELIST) and (str(filename).endswith("txt.zip")):
                print(f"Skipping file {filename} with unsupported extension {file_ext}.")
                continue

            filename = Path(file["filepath"]).name
            print(f"Project: {project}, interesting File: {filename}")

            url = file["filepath"]
            path = await download_ftp(
                url=url, out_dir=Path("C:/Users/Nils/Desktop/MP/usigrabber/data/files")
            )
            new_path_name = (
                str(os.path.dirname(path)) + "/" + project + "_" + os.path.basename(file_url)
            )
            os.rename(path, Path(new_path_name))

    # url = files["search"][0]["filepath"]

    # path = await download_ftp(url=url, out_dir=DATA_DIR / "files")

    # print(f"Downloaded to: {path}")

    # DATA_DIR = Path("C:/Users/Nils/Desktop/MP/usiGrabber/data")

    # path = Path(DATA_DIR / "files" / "txt" / "allPeptides.txt")

    # df = pd.read_csv(path, sep="\t")
    # print(df.head())
    # df_subset = df.head(1000)
    # df_subset.to_csv(DATA_DIR / "files" / "txt" / "allPeptides.csv", index=False)


asyncio.run(main())
