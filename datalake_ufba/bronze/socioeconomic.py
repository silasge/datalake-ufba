import click
import pandas as pd

from datalake_ufba.utils import import_df_to_duckdb


def clean_bronze_socioeconomic(socioeconomic_df: pd.DataFrame) -> pd.DataFrame:
    # drop "filter_$" column
    if "filter_$" in socioeconomic_df.columns:
        socioeconomic_df = socioeconomic_df.drop(columns="filter_$")

    # all columns to lowercase
    socioeconomic_df = socioeconomic_df.rename(columns=lambda x: x.lower())

    # convert "insrica" and "cpf" to str
    for column in ["inscrica", "cpf"]:
        if column in socioeconomic_df.columns:
            socioeconomic_df[column] = socioeconomic_df[column].astype(str)

    columns_to_test_for_dups = ["ano", "inscrica", "area", "curso"]

    if "cpf" in socioeconomic_df.columns:
        columns_to_test_for_dups = columns_to_test_for_dups + ["cpf"]

    return socioeconomic_df.drop_duplicates(subset=columns_to_test_for_dups)


@click.command()
@click.option("-se", "--socioeconomic-path", required=True, type=str)
@click.option("-db", "--duckdb-path", required=True, type=str)
def import_socioeconomic_to_bronze(socioeconomic_path: str, duckdb_path: str) -> None:
    # getting the name of the table
    table_name = socioeconomic_path.split("/")[-1].split(".")[0].lower()

    # read the file
    df = pd.read_spss(path=socioeconomic_path)

    # clean dataframe
    df = clean_bronze_socioeconomic(socioeconomic_df=df)

    # import to duckdb
    import_df_to_duckdb(
        df=df, duckdb_path=duckdb_path, schema="ufba_bronze", table=table_name
    )
