import pandas as pd	
from .logger import logger


def transform_customer(df):
    logger.info('ETL start')

    orignal_count=len(df)

    #duplicate validation

    df_mask=df.duplicated(keep='first')

    df_invalid_duplicated=df[df_mask].copy()
    df_invalid_duplicated['error_reason']='Duplicated Records'

    df_valid=df[~df_mask].copy()

    duplire=orignal_count-len(df)
    logger.info(f'total records is {orignal_count}')
    logger.info(f'duplicate records found is len{df_invalid_duplicated}')

    #NULL customer_id validation

    null_mask=df_valid['customer_id'].isnull()

    df_invalid_null=df_valid[null_mask].copy()
    df_invalid_null['error_reason']='Customer_id is Null'

    df_valid=df[~null_mask].copy()

    logger.info(f'NULL customer_id records found: {len(df_invalid_null)}')

    #combine records
    df_invalid=pd.concat([df_invalid_duplicated,df_invalid_null],ignore_index=True)

    logger.info(f'valid records is {len(df_valid)}')
    logger.info(f'invalid records is {len(df_invalid)}')

    return df_valid,df_invalid
