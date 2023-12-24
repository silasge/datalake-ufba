import os

import click
import polars as pl

from datalake_ufba.utils import import_df_to_duckdb


@click.command()
@click.option("-si", "--sisu-path", required=True, type=str)
@click.option("-db", "--duckdb-path", required=True, type=str)
def import_sisu_to_bronze(sisu_path: str, duckdb_path: str) -> None:
    # geting the name of the table
    table_name = os.path.split(sisu_path)[-1].split(".")[0]

    # read the file
    df = pl.read_csv(source=sisu_path, infer_schema_length=None)

    # import to duckdb
    import_df_to_duckdb(
        df=df, duckdb_path=duckdb_path, schema="ufba_bronze", table=table_name
    )
