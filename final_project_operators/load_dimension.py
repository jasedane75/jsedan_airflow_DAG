from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    sql_insert = """
        INSERT INTO {}
        {};
    """


    @apply_defaults
    def __init__(self,
                 redshift="",
                 table="",
                 sql="",
                 truncate= False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):

        redshift_exec = PostgresHook(postgres_conn_id = self.redshift)

        if self.truncate:
            self.log.info(f"Limpiando la tabla: {self.table}")
            redshift_exec.run(f"TRUNCATE TABLE {self.table}") 

        self.log.info(f"Carga de tabla dimensional '{self.table}' en Redshift")
        sql_formateado = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql
        )

        redshift_exec.run(sql_formateado)
