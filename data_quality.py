from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ="",
                 table ="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)  
        
        for table in self.tables:
            self.log.info('DataQualityOperator')
            redshift_hook = PostgresHook(self.redshift_conn_id)  
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
            if len(records) < 1 or len(records[0]) < 1:
               raise ValueError(f"Data quality check failed. {self.table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
