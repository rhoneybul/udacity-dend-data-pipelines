from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 sql_statement,
                 target_table,
                 delete_load=True,
                 redshift_conn_id='amazon-redshift',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql_statement = sql_statement
        self.target_table = target_table
        self.delete_load = delete_load
        self.redshift_conn_id = redshift_conn_id
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        try:
            redshift_hook = PostgresHook(self.redshift_conn_id)
            sql_load_statement = '''
            {}
            insert into {}
            {}
            '''.format(f'delete from {self.target_table};' if self.delete_load else '',
                       self.target_table, 
                       self.sql_statement)
            logging.info(f"running load dimension::sql_statement::{sql_load_statement}")
            redshift_hook.run(sql_load_statement)
        except Exception as e:
            raise e
