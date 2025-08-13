import os
import sys
import time
import logging
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm
from dwca import DarwinCoreArchive
from dwca.classes import DataFile
from psycopg2.extras import execute_batch
from psycopg2.extensions import connection

BATCH_SIZE = 1000

def create_table(conn: connection, data_file: DataFile, replace: bool = False) -> None:
    statement = data_file.sql_table
    if replace:
        statement = statement.replace("CREATE TABLE \"Occurrence\"", "CREATE TABLE \"Verbatim\"")
    with conn.cursor() as cursor:
        cursor.execute(statement)
    conn.commit()
    return

def insert_data(conn: connection, data_file: DataFile, replace: bool = False) -> None:
    with conn.cursor() as cursor:
        values = list()
        for statement, value in tqdm(data_file.insert_sql, total=len(data_file), file=sys.stdout):
            values.append(value)
            if len(values) >= BATCH_SIZE:
                if replace:
                    statement = statement.replace("INSERT INTO \"Occurrence\"", "INSERT INTO \"Verbatim\"")
                execute_batch(cursor, statement, values)
                values.clear()
                conn.commit()
        if len(values) > 0:
            if replace:
                statement = statement.replace("INSERT INTO \"Occurrence\"", "INSERT INTO \"Verbatim\"")
            execute_batch(cursor, statement, values)
            values.clear()
            conn.commit()
    conn.commit()
    return

def main() -> None:
    gbif_chile = DarwinCoreArchive.from_file("data/chile-dwca-var.zip", lazy=True)
    if load_dotenv():
        logging.info("Environmental variables loaded")
    else:
        logging.info("Error importing env var")
    conn = psycopg2.connect(
        host=os.getenv("LOCAL_HOST"),
        port=os.getenv("LOCAL_PORT"),
        user=os.getenv("LOCAL_USER"),
        password=os.getenv("LOCAL_PASS"),
        dbname=os.getenv("LOCAL_DB"),
    )
    conn.rollback()
    create_table(conn, gbif_chile.core)
    for extension in gbif_chile.extensions:
        create_table(conn, extension, replace=True)
    conn.commit()
    start = time.time()
    insert_data(conn, gbif_chile.core)
    logging.info(f"Insert {gbif_chile.core.filename} took {time.time() - start:.2f} seconds")
    for extension in gbif_chile.extensions:
        start = time.time()
        insert_data(conn, extension, replace=True)
        logging.info(f"Insert {extension.filename} took {time.time() - start:.2f} seconds")
    gbif_chile.core.close()
    for extension in gbif_chile.extensions:
        extension.close()
    return

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
