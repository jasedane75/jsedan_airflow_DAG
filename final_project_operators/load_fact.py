from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    sql_insert = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift
        self.table = table
        self.sql = sql

    def execute(self, context):

        redshift_exec = PostgresHook(postgres_conn_id = self.redshift)
        self.log.info(f"Carga de tabla '{self.table}' en Redshift")
        sql_ajustado = LoadFactOperator.sql_insert.format(
            self.table,
            self.sql
        )
        redshift_exec.run(sql_ajustado)
