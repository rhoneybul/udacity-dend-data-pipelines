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
                 redshift_conn_id='amazon-redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        # aws_hook = AwsHook(aws_conn_id)
        # credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        sql_load_statement='''
        insert into {}
        {}
        '''.format(self.target_table, 
                   sql_statement)
        redshift_hook.run(sql_load_statement)
