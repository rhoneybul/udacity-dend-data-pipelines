from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 #  Example:
                 #     redshift_conn_id=your-connection-name
                 s3_file_path,
                 target_table,
                 file_type,
                 columns,
                 data_format='auto',
                 redshift_conn_id='amazon-redshift',
                 aws_conn_id='amazon-s3',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.s3_file_path = s3_file_path
        self.target_table = target_table
        self.file_type = file_type 
        self.columns = columns
        self.data_format = data_format
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        # self.log.info('StageToRedshiftOperator not implemented yet')
        try:
            sql_statement = '''
            DELETE FROM '{}'
            COPY {}
            ({})
            FROM '{}'
            FORMAT {} as '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            '''

            aws_hook = AwsHook(self.aws_conn_id)
            credentials = aws_hook.get_credentials()

            logging.info('Using AWS Credentials')
            logging.info(f'ACCESS_KEY: {credentials.access_key}')
            logging.info(f'SECRET KEY: {credentials.secret_key}')

            sql_execute_statement = sql_statement.format(self.target_table,
                                                         self.target_table,
                                                         ', '.join(self.columns),
                                                         self.s3_file_path,
                                                         self.file_type,
                                                         self.data_format,
                                                         credentials.access_key,
                                                         credentials.secret_key)

            logging.info(f'using sql statement: {sql_execute_statement}')

            redshift_hook = PostgresHook(self.redshift_conn_id)
            redshift_hook.run(sql_execute_statement)
        except Exception as e:
            logging.error(e)
            raise e





