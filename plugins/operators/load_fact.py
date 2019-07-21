from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

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

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.sql_statement = sql_statement
        self.delete_load=delete_load
        self.target_table = target_table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # aws_hook = AwsHook(aws_conn_id)
        # credentials = aws_hook.get_credentials()
        try:
            redshift_hook = PostgresHook(self.redshift_conn_id)
            sql_load_statement='''
            delete from {};
            insert into {}
            {}
            '''.format(f'delete from {self.target_table};' if self.delete_load else ''
                       self.target_table,
                       self.target_table, 
                       self.sql_statement)
            logging.info(f'executing load fact table::sql query::{sql_load_statement}')
            redshift_hook.run(sql_load_statement)
        except Exception as e:
            raise e
