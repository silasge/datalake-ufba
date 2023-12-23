import os

import click
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


def get_gauth() -> None:
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)
    return drive


def get_file_from_gdrive(gauth, id_, save_as) -> None:
    file = gauth.CreateFile({"id": id_})
    file.GetContentFile(save_as)


@click.command()
@click.option("-i", "--id", "id_", required=True, type=str)
@click.option("-p", "--path", required=True, type=str)
def download_file_from_gdrive(id_, path):
    path_splits = os.path.split(path)
    gauth = get_gauth()
    get_file_from_gdrive(gauth=gauth, id_=id_, save_as=path)
    click.echo(f"File {path_splits[-1]} downloaded at {path_splits[0]}.")
