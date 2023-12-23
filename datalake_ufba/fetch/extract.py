import os

import click
import py7zr


@click.command()
@click.option("-7z", "--7zip_file", "_7z_file", required=True, type=str)
def extract_7z(_7z_file: str) -> None:
    with py7zr.SevenZipFile(_7z_file, "r") as archive:
        archive.extractall(path=os.path.dirname(_7z_file))
