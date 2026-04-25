from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift,
                 tests,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift
        self.tests = tests


    def execute(self, context):
        
        self.log.info('DataQualityOperator implemented')

        redshift_exec = PostgresHook(postgres_conn_id = self.redshift)    

        for test in self.tests:
            
            self.log.info(f"Ejecutando prueba: {test['check_sql']}")
            result = redshift_exec.get_first(test['check_sql'])
            
            """Validaciones relacionadas a la integridad y calidad de los datos"""
            
            if result is None or len(result) == 0:
                self.log.info(f"Validacion de resultados fallida en: {test['check_sql']}")
                raise ValueError(f"Prueba no devolvió resultados: {test['check_sql']}")
                
            if 'expected_value' in test and result[0] != test['expected_value']:
                self.log.info(f"Validacion de resultados fallida en: {test['check_sql']}")
                raise ValueError(
                    f"Prueba falló: {test['check_sql']}. "
                    f"Esperado {test['expected_value']}, obtenido {result[0]}"
                )

            if 'expected_min' in test and result[0] < test['expected_min']:
                self.log.info(f"Validacion de resultados fallida en: {test['check_sql']}")
                raise ValueError(
                    f"Prueba falló: {test['check_sql']}. "
                    f"Mínimo esperado {test['expected_min']}, obtenido {result[0]}"
                )

            self.log.info(f"Prueba exitosa: {test['check_sql']}")
