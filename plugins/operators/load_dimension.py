from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

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

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        aws_hook=AwsHook(aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")
        sql_load_statement = '''
        insert into {}
        {}
        '''.format(target_table, 
                   sql_statement,
                   credentials.access_key,
                   credentials.secret_key)
        redshift_hook.run(sql_statement)
