from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 #  Example:
                 #     redshift_conn_id=your-connection-name
                 s3_file_path,
                 target_table,
                 file_type='.csv'
                 redshift_conn_id='amazon-redshift',
                 aws_conn_id='amazon-s3',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        # self.log.info('StageToRedshiftOperator not implemented yet')

        sql_statement = '''
        COPY {}
        FROM {}
        WITH (FORMAT {})
        '''.format(target_table, s3_file_path, file_type)

        aws_hook = AwsHook(aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run(sql_statement)





