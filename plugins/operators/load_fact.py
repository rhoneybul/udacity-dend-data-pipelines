from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 sql_statement,
                 target_table,
                 aws_conn_id='aws-connection',
                 redshift_conn_id='amazon-redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook('redshift')
        sql_load_statement='''
        insert into {}
        {}
        AWS_ACCESS_KEY_ID '{{}}'
        SECRET_ACCESS_KEY '{{}}'
        '''.format(target_table, 
                   sql_statement,
                   credentials.access_key,
                   credentials.secret_key)
        redshift_hook.run(sql_load_statement)
