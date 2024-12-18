import sqlalchemy as sa
import json


def postgres_engine(
        username,
        password,
        host='osso-postgres-db.cxi4au6ok50u.us-east-1.rds.amazonaws.com',
        port='5432',
        database='postgres'
) -> sa.engine.base.Engine:
    """
    Create a SQLAlchemy engine object to be used in queries
    :param username:
    :param password:
    :param host: Default 'osso-postgres-db.cxi4au6ok50u.us-east-1.rds.amazonaws.com'
    :param port: Default '5432'
    :param database: Default 'postgres'
    :return:
    """
    url_object = sa.URL.create(
        drivername='postgresql',
        username=username,
        password=password,
        host=host,
        port=port,
        database=database
    )
    engine = sa.create_engine(url_object, connect_args={'sslmode': 'require'})
    return engine


def postgres_select_query(engine: sa.engine.base.Engine, query: str) -> sa.Sequence:
    """
    Uses SQLAlchemy engine object to establish a connection and execute a SELECT query
    :param engine:
    :param query:
    :return: query results in list of dicts
    """
    connection = engine.connect()
    query = sa.text(query)
    results = connection.execute(query).fetchall()
    connection.close()
    return [r[0] for r in results]


def df_upsert(data_frame, table_name, engine, schema=None, match_columns=None, insert_only=False):
    """
    Perform an "upsert" on a PostgreSQL table from a DataFrame.
    Constructs an INSERT … ON CONFLICT statement, uploads the DataFrame to a
    temporary table, and then executes the INSERT.
    Parameters
    ----------
    data_frame : pandas.DataFrame
        The DataFrame to be upserted.
    table_name : str
        The name of the target table.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy Engine to use.
    schema : str, optional
        The name of the schema containing the target table.
    match_columns : list of str, optional
        A list of the column name(s) on which to match. If omitted, the
        primary key columns of the target table will be used.
    insert_only : bool, optional
        On conflict do not update. (Default: False)
    """
    table_spec = ""
    if schema:
        table_spec += '"' + schema.replace('"', '""') + '".'
    table_spec += '"' + table_name.replace('"', '""') + '"'

    df_columns = list(data_frame.columns)
    if not match_columns:
        insp = sa.inspect(engine)
        match_columns = insp.get_pk_constraint(table_name, schema=schema)[
            "constrained_columns"
        ]
    columns_to_update = [col for col in df_columns if col not in match_columns]
    insert_col_list = ", ".join([f'"{col_name}"' for col_name in df_columns])
    stmt = f"INSERT INTO {table_spec} ({insert_col_list})\n"
    stmt += f"SELECT {insert_col_list} FROM temp_table\n"
    match_col_list = ", ".join([f'"{col}"' for col in match_columns])
    stmt += f"ON CONFLICT ({match_col_list}) DO "
    if insert_only:
        stmt += "NOTHING"
    else:
        stmt += "UPDATE SET\n"
        stmt += ", ".join(
            [f'"{col}" = EXCLUDED."{col}"' for col in columns_to_update]
        )

    with engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS temp_table")
        conn.exec_driver_sql(
            f"CREATE TEMPORARY TABLE temp_table AS SELECT * FROM {table_spec} WHERE false"
        )
        data_frame.to_sql("temp_table", conn, if_exists="append", index=False)
        cursor_result = conn.exec_driver_sql(stmt)
    return cursor_result


if __name__ == '__main__':
    with open("../creds.json", "r") as f:
        f_data = json.load(f)
        postgres_username = f_data[0]['postgres_username']
        postgres_password = f_data[0]['postgres_password']

    con = postgres_engine(username=postgres_username, password=postgres_password)
    print(con)
    res_dict = postgres_select_query(engine=con, query="SELECT key FROM jira_issues")
    con.dispose()
