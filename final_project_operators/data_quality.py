from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift
        self.tables = tables


    def execute(self, context):
        
        self.log.info('DataQualityOperator implemented')

        redshift_exec = PostgresHook(postgres_conn_id = self.redshift)    

        for table in self.tables:
            registros = redshiftexec.get_records(f'SELECT COUNT(*) FROM {table}') 

            if len(registros) < 1 or len(registros[0]) < 1 or registros[0][0] == 0:
                self.log.error( f'Validaciones de calidad fallidas: Tabla "{table}" esta vacía')
                raise ValueError( f'Validaciones de calidad fallidas: table "{table}" esta vacía')
            self.log.info(f'Validaciones exitosas en registros de la tabla "{table}" con un total de {registros[0][0]} registros')

