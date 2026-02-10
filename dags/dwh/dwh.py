from dwh.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from dwh.data_loading import load_data
from dwh.data_modification import insert_rows, update_rows, delete_rows
from dwh.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    schema = 'staging'
    con, cur = None, None

    try: 
        conn, cur = get_conn_cursor()
        yt_data = load_data()
        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        for row in yt_data:
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)

        ids_in_json = { row['video_id'] for row in yt_data }
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, row)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error happened with update of {schema}.{table} table: ", e)
        raise

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

@task
def core_table():
    schema = 'core'
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)
        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table}")
        rows = cur.fetchall()

        for row in rows:
            current_video_ids.add(row['video_id'])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
            else:
                transformed_row = transform_data(row)

                if transformed_row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema}.{table} table update is completed.")

    except Exception as e:
        logger.error(f"An error happened during update of {schema}.{table} table: {e}")
        raise
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)