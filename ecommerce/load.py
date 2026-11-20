
import pyodbc
import configparser
from .logger import logger


def get_connection():
    try:
        config = configparser.ConfigParser()
        config.read('/opt/airflow/config/config.ini')

        server = config['SQL_SERVER']['server']
        database = config['SQL_SERVER']['database']
        username = config['SQL_SERVER']['username']
        password = config['SQL_SERVER']['password']

        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
        )

        logger.info("SQL Server connection successful")
        return conn

    except Exception as e:
        logger.exception(f"SQL connection failed: {e}")
        raise


def load_valid_customer(df_valid, conn):

    cursor = None

    try:
        cursor = conn.cursor()

        query = """
        MERGE dbo.customers_valid AS target
        USING (
            VALUES (?, ?, ?, ?, ?)
        ) AS source (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        )
        ON target.customer_id = source.customer_id

        WHEN MATCHED THEN
            UPDATE SET
                target.customer_unique_id = source.customer_unique_id,
                target.customer_zip_code_prefix = source.customer_zip_code_prefix,
                target.customer_city = source.customer_city,
                target.customer_state = source.customer_state

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state
            )
            VALUES (
                source.customer_id,
                source.customer_unique_id,
                source.customer_zip_code_prefix,
                source.customer_city,
                source.customer_state
            );
        """

        data = [
            (
                row.customer_id,
                row.customer_unique_id,
                row.customer_zip_code_prefix,
                row.customer_city,
                row.customer_state
            )
            for row in df_valid.itertuples(index=False)
        ]

       # customer_id=df_valid['customer_id']
       # customer_city=df_valid['customer_city']

        logger.info(f"Records prepared for merge: {len(data)}")
        # print(f'id is change that is {customer_id} and chnage city as {customer_city}')
        if data:
            cursor.fast_executemany = True
            cursor.executemany(query, data)
            conn.commit()

            logger.info(f"Rows inserted: {cursor.rowcount}")
        else:
            logger.info("No valid records available to insert")

    except Exception as e:
        conn.rollback()
        logger.exception(f"Valid customer insert failed: {e}")
        raise

    finally:
        if cursor:
            cursor.close()


def load_invalid_customer(df_invalid, conn):

    cursor = None

    try:
        cursor = conn.cursor()

        query = """
        INSERT INTO dbo.customers_invalid (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        )
        VALUES (?, ?, ?, ?, ?)
        """

        data = [
            (
                row.customer_id,
                row.customer_unique_id,
                row.customer_zip_code_prefix,
                row.customer_city,
                row.customer_state
            )
            for row in df_invalid.itertuples(index=False)
        ]

        if data:
            cursor.fast_executemany = True
            cursor.executemany(query, data)
            conn.commit()

            logger.info(f"Invalid rows inserted: {cursor.rowcount}")
        else:
            logger.info("No invalid records available to insert")

    except Exception as e:
        conn.rollback()
        logger.exception(f"Invalid customer insert failed: {e}")
        raise

    finally:
        if cursor:
            cursor.close()

def deleted_customer(conn,df_deleted):
    cursor=None
    try:
        
        cursor=conn.cursor()

        query='''delete from customers_valid where customer_id=?'''
        #below line selects only the customer_id column from dataframe,dropna remove null value in custid col,str(customer_id),this is single value tuple (101,)
        customer_ids=[(str(customer_id),)for customer_id in df_deleted['customer_id'].dropna()]

        if customer_ids:
            cursor.executemany(query,customer_ids)

            conn.commit()
            logger.info(f'deleted records proceeds len is {len(customer_ids)}')

        else:
            logger.info('no deleted records found')
    except Exception as e:
        conn.rollback()
        logger.exception(f'customer delted fail {e}')
        raise

    finally:
        if cursor:
            cursor.close()

        
    

    
