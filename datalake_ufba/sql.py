import click
import duckdb


@click.command()
@click.option("-sql", "--sql_file", required=True, type=str)
@click.option("-db", "--duckdb_path", required=True, type=str)
def execute_sql_file(sql_file: str, duckdb_path: str) -> None:
    # init duckdb connection
    conn = duckdb.connect(duckdb_path)

    # read sql file
    with open(sql_file) as f:
        sql = f.read()

    # execute sql_file and close connection
    conn.sql(sql)
    conn.close()
