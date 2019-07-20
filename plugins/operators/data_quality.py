from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 tables,
                 redshift_conn_id='aws-redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            logging.info(f'verifying data produced for {table}')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            else:
                logging.info(f'data verified for {table}')
        
        
