import logging
import os

LOG_DIR = '/opt/airflow/logs/ecommerce'

os.makedirs(LOG_DIR,exist_ok=True)

logging.basicConfig(filename=os.path.join(LOG_DIR, 'report.log'),
                   level=logging.INFO,
                   format='%(asctime)s-%(levelname)s-%(message)s')

logger=logging.getLogger('ecommerce')
