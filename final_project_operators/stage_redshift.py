from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift="",
                 aws_credentials="",
                 table="",
                 bucket_s3="",
                 s3_key="",
                 s3_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift
        self.aws_credentials = aws_credentials
        self.table = table
        self.bucket_s3 = bucket_s3
        self.s3_key = s3_key
        self.s3_format = s3_format    


    def execute(self, context):

        metastoreBackend = MetastoreBackend()

        aws_connection = metastoreBackend.get_connection(self.aws_credentials)
        redshift_exec = PostgresHook(postgres_conn_id=self.redshift)

        self.log.info("Eliminación de la tabla en Redshift")
        redshift_exec.run("DELETE FROM {}".format(self.table))

        self.log.info("Copiar data desde S3 a Redshift")
        
        rendered_key = self.s3_key.format(**context)
        
        s3_path = "s3://{}/{}".format(self.bucket_s3, rendered_key)
        
        formated_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.s3_format
        )

        redshift_exec.run(formated_sql)       


