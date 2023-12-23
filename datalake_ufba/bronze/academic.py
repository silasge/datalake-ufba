from hashlib import sha256

import click
import polars as pl

from datalake_ufba.utils import import_df_to_duckdb


def get_academic_schema():
    return {
        "cpf": pl.Utf8,
        "mtr": pl.Utf8,
        "inscrica": pl.Utf8,
        "nome": pl.Utf8,
        "per_ingr": pl.Float64,
        "cd_forma_ingr": pl.Float64,
        "descr_forma_ingr": pl.Utf8,
        "per_saida": pl.Float64,
        "cd_forma_saida": pl.Float64,
        "descr_forma_saida": pl.Utf8,
        "cr": pl.Float64,
        "escore": pl.Float64,
        "class_geral": pl.Float64,
        "categoria_class": pl.Utf8,
        "cod_curso": pl.Float64,
        "per_crs_ini": pl.Float64,
        "nome_curso": pl.Utf8,
        "colegiado": pl.Float64,
        "col_nm_colegiado": pl.Utf8,
        "per_let_disc": pl.Float64,
        "disc": pl.Utf8,
        "ch_disc": pl.Float64,
        "nat_disc": pl.Utf8,
        "tur": pl.Utf8,
        "nota": pl.Float64,
        "resultado": pl.Utf8,
        "doc_nu_matricula_docente": pl.Utf8,
        "doc_nm_docente": pl.Utf8,
        "doc_vinculo": pl.Utf8,
        "doc_titulacao": pl.Utf8,
        "doc_nivel": pl.Utf8,
        "doc_regime_trab": pl.Utf8,
        "nascimento": pl.Utf8,
        "aln_cd_estado_civil": pl.Utf8,
        "ecv_ds_estado_civil": pl.Utf8,
        "sexo": pl.Utf8,
        "dtnasc": pl.Utf8,
        "aln_sg_estado_nascimento": pl.Utf8,
        "aln_nm_pai": pl.Utf8,
        "aln_nm_mae": pl.Utf8,
        "aln_cd_cor": pl.Utf8,
        "cor_nm_cor": pl.Utf8,
        "aln_nm_cidade_nascimento": pl.Utf8,
        "eda_nm_email": pl.Utf8,
        "pk_id_academic": pl.Utf8,
    }


def clean_bronze_academic(academic_df: pl.LazyFrame) -> pl.LazyFrame:
    pk_cols = [
        "cpf",
        "mtr",
        "inscrica",
        "per_ingr",
        "per_let_disc",
        "disc",
        "nota",
        "resultado",
        "doc_nu_matricula_docente",
        "doc_nm_docente",
        "doc_vinculo",
        "doc_titulacao",
        "doc_nivel",
        "doc_regime_trab",
    ]

    academic_df = academic_df.with_columns(
        pl.concat_str(pl.col(pk_cols).cast(pl.Utf8).fill_null("NULL"), separator="-")
        .map_elements(lambda x: sha256(x.encode()).hexdigest())
        .alias("pk_id_academic")
    ).unique(subset="pk_id_academic")

    return academic_df


@click.command()
@click.option("-ac", "--academic-path", required=True, type=str)
@click.option("-db", "--duckdb-path", required=True, type=str)
def import_academic_to_bronze(academic_path: str, duckdb_path: str) -> None:
    schemas = get_academic_schema()

    # read the file
    # df = pl.scan_csv(source=academic_path, dtypes=schemas)
    reader = pl.read_csv_batched(source=academic_path, dtypes=schemas)
    batches = reader.next_batches(10)

    num_of_batches = 1

    while batches:
        df_batches = pl.concat(batches)

        # clean dataframe
        df_batches = clean_bronze_academic(academic_df=df_batches)
        # import to duckdb
        import_df_to_duckdb(
            df=df_batches,
            duckdb_path=duckdb_path,
            schema="ufba_bronze",
            table="ufba_academica_0321",
        )
        click.echo(f"Batch {num_of_batches} imported.")
        batches = reader.next_batches(10)
        num_of_batches += 1
