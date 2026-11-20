from airflow.sdk import dag,task
import pendulum
from ecommerce.extract import get_connection,get_last_version,extractdata,fullload,update_watermark
from ecommerce.transform import transform_customer
from ecommerce.load import get_connection,load_valid_customer,load_invalid_customer,deleted_customer
import pandas as pd
import os
import uuid

@dag(dag_id='ecommarce_dag_csv',schedule=None,start_date=pendulum.now('Asia/Kolkata'),catchup=True)

def ecommarce_dag_csv():
    @task.python
    
    def extract_task():

        conn = None

        try:
            conn = get_connection()

            print("SQL Server connection successful")

            # Extract full data from SQL Server
            #df = fullload(conn)

            #Extract incremental data
            last_version=get_last_version(conn)
            print(f'last process version is {last_version}')

            #get currunt version
            cursor=conn.cursor()
            cursor.execute('SELECT CHANGE_TRACKING_CURRENT_VERSION()')
            current_version=cursor.fetchone()[0]
            cursor.close()            
            print(f'current version is {current_version}')
            
            df = extractdata(conn,last_version,current_version)

            print(f"Total extracted records: {len(df)}")

            # Create data folder
            output_dir = "/opt/airflow/data/ecommerce"
            os.makedirs(output_dir, exist_ok=True)

            # Generate unique file name
            file_path = os.path.join(
                output_dir,
                f"raw_customers_{uuid.uuid4().hex}.csv"
            )

            # Save DataFrame into CSV
            df.to_csv(file_path, index=False)

            print(f"Raw data saved successfully: {file_path}")

            # Return only file path through XCom
            return {"file_path": file_path,"current_version": current_version
}

        except Exception as e:
            print(f"Extract task failed: {e}")
            raise

        finally:
            if conn:
                conn.close()

    
    @task.python
    def transform_task(extract_result):

        try:
            # Read raw CSV file
            file_path=extract_result['file_path']
            current_version=extract_result['current_version']
            
            df = pd.read_csv(file_path)

            df_deleted=df[df['choperation']=='D'].copy()
            df_changes=df[df['choperation'].isin(['U','I'])].copy()
            
            print(f'deleted records:{len(df_deleted)}')
            print(f"insert update records: {len(df_changes)}")

            # Perform validation
            df_valid, df_invalid = transform_customer(df_changes)

            print(f"Valid records: {len(df_valid)}")
            print(f"Invalid records: {len(df_invalid)}")

            output_dir = "/opt/airflow/data/ecommerce"
            # Save transformed data
            os.makedirs(output_dir, exist_ok=True)

            valid_path = os.path.join(
                output_dir,
                f"valid_customers_{uuid.uuid4().hex}.csv"
            )

            invalid_path = os.path.join(
                output_dir,
                f"invalid_customers_{uuid.uuid4().hex}.csv"
            )

            delete_path=os.path.join(output_dir,f'deleted_customer_{uuid.uuid4().hex}.csv')
            
            df_valid.to_csv(valid_path, index=False)
            df_invalid.to_csv(invalid_path, index=False)
            df_deleted.to_csv(delete_path,index=False)
            
            print(f"Valid data saved: {valid_path}")
            print(f"Invalid data saved: {invalid_path}")
            print(f"Deleted data saved: {delete_path}")

            # Return only file paths through XCom
            return {
                "valid_path": valid_path,
                "invalid_path": invalid_path,
                "current_version":current_version,
                "Deleted_path":delete_path
            }

        except Exception as e:
            print(f"Transform task failed: {e}")
            raise


    @task.python
    def load_task(transform_result):

        conn = None

        try:
            valid_path = transform_result["valid_path"]
            invalid_path = transform_result["invalid_path"]
            current_version=transform_result["current_version"]
            delete_path=transform_result["Deleted_path"]
            
            # Read valid and invalid CSV files
            df_valid = pd.read_csv(valid_path)
            df_invalid = pd.read_csv(invalid_path)
            df_deleted=pd.read_csv(delete_path)

            print(f"Valid records to load: {len(df_valid)}")
            print(f"Invalid records to load: {len(df_invalid)}")

            # Connect to SQL Server
            conn = get_connection()

            # Load valid records
            load_valid_customer(df_valid, conn)

            # Load invalid records
            load_invalid_customer(df_invalid, conn)

            #delete records
            deleted_customer(conn,df_deleted)
            
            print("Data loading completed successfully")

             # Update watermark only after successful loads
            update_watermark(conn, current_version)
            
        except Exception as e:
            print(f"Load task failed: {e}")
            raise

        finally:
            if conn:
                conn.close()

    extract=extract_task()
    transform=transform_task(extract)
    load=load_task(transform)
    
   
ecommarce_dag_csv()
