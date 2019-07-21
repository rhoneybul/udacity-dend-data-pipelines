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
                 redshift_conn_id='amazon-redshift',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql_statement = sql_statement
        self.target_table = target_table
        self.redshift_conn_id = redshift_conn_id
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        try:
            redshift_hook = PostgresHook(self.redshift_conn_id)
            sql_load_statement = '''
            delete from {};
            insert into {}
            {}
            '''.format(self.target_table,
                       self.target_table, 
                       self.sql_statement)
            redshift_hook.run(sql_statement)
        except Exception as e:
            raise e
