import asyncio
from pathlib import Path

import pandas as pd


async def main():
    # files = pride.PrideBackend.get_files_for_project("PXD068559")

    # url = files["search"][0]["filepath"]

    # path = await download_ftp(url=url, out_dir=DATA_DIR / "files")

    # print(f"Downloaded to: {path}")

    DATA_DIR = Path("C:/Users/Nils/Desktop/MP/usiGrabber/data")

    path = Path(DATA_DIR / "files" / "txt" / "allPeptides.txt")

    df = pd.read_csv(path, sep="\t")
    print(df.head())
    df_subset = df.head(1000)
    df_subset.to_csv(DATA_DIR / "files" / "txt" / "allPeptides.csv", index=False)


asyncio.run(main())
