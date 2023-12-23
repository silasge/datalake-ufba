import duckdb
import pandas as pd
import polars as pl


def import_df_to_duckdb(
    df: pd.DataFrame | pl.DataFrame | pl.LazyFrame,
    duckdb_path: str,
    schema: str,
    table: str,
) -> None:
    # test if "df" is indeed a valid dataframe
    if not isinstance(df, (pd.DataFrame, pl.DataFrame, pl.LazyFrame)):
        raise Exception("Not a valid DataFrame.")

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    # init duckdb connection
    conn = duckdb.connect(duckdb_path)

    # because the df is given as a argument to a function, we need to register it
    # so duckdb can find it inside the function
    duckdb.register("df", df)

    # now, we can define our sql statement, which will inser the df into duckdb
    sql = f"""
    INSERT OR IGNORE INTO {schema}.{table}
    SELECT * FROM df;
    """

    # finally, we can just execute the sql statement defined above
    # and close the connection
    conn.sql(sql)
    conn.commit()
    conn.close()
