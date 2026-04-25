
# Sparkify - Pipeline de Datos con Airflow, S3 y Redshift

## Introducción

Sparkify es una startup de streaming musical que ha crecido tanto en usuarios como en catálogo de canciones. Con este crecimiento, surge la necesidad de automatizar y estructurar el flujo de datos para que el equipo de analítica pueda responder preguntas clave del negocio: ¿qué canciones escuchan los usuarios?, ¿en qué horarios hay mayor actividad?, ¿cuáles son los artistas más populares?

Este proyecto construye un pipeline de datos automatizado que toma la información cruda almacenada en Amazon S3, la transforma y la organiza en un modelo analítico dentro de Amazon Redshift, todo orquestado por Apache Airflow.

## ¿Cómo funciona?

Los datos de Sparkify viven en dos conjuntos principales dentro de S3: los registros de actividad de los usuarios (qué canciones reproducen, cuándo y desde dónde) y el catálogo de canciones con información de artistas, duración y ubicación geográfica.

El pipeline, definido como un DAG en Airflow, se ejecuta cada hora y sigue este flujo:

1. Primero se crean las tablas necesarias en Redshift si aún no existen.
2. Luego se copian los datos crudos desde S3 hacia dos tablas staging en Redshift, una para eventos de usuario y otra para canciones.
3. A partir de esas tablas staging, se alimenta la tabla de hechos **songplays**, que registra cada reproducción de una canción.
4. Después se cargan las tablas de dimensiones: **users**, **songs**, **artists** y **time**, que permiten analizar las reproducciones desde distintas perspectivas.
5. Finalmente, se ejecutan validaciones de calidad para garantizar que todas las tablas contengan datos y que el proceso haya sido exitoso.

Si algún paso falla, Airflow reintenta automáticamente hasta 3 veces con intervalos de 5 minutos, lo que le da robustez al proceso.

## Arquitectura

La arquitectura se apoya en tres servicios principales:

- **Amazon S3** actúa como el lago de datos donde se almacena la información cruda en formato JSON. Es el punto de partida del pipeline.
- **Amazon Redshift** es el data warehouse donde se estructura la información en un modelo de estrella, optimizado para consultas analíticas.
- **Apache Airflow** es el orquestador que coordina todo el flujo, controla dependencias entre tareas, gestiona reintentos y permite monitorear la ejecución desde su interfaz web.

Para conectar estos servicios, se desarrollaron operadores personalizados en Python que encapsulan la lógica de cada etapa: carga desde S3, inserción en tablas de hechos y dimensiones, y validaciones de calidad.

## Modelo de datos

Se implementó un modelo de estrella centrado en la tabla de hechos **songplays**, que registra cada vez que un usuario reproduce una canción. Alrededor de esta tabla se encuentran cuatro dimensiones que enriquecen el análisis:

- **users**: información de los usuarios de Sparkify.
- **songs**: detalle de cada canción del catálogo.
- **artists**: datos de los artistas musicales.
- **time**: desglose temporal de las reproducciones (hora, día, semana, mes, año).

Este diseño permite al equipo de analítica hacer consultas eficientes como cruzar reproducciones por artista en un rango de fechas, o identificar patrones de uso por hora del día.

## Resumen

El proyecto implementa un pipeline ETL completo y automatizado para Sparkify. Los datos pasan de ser archivos JSON dispersos en S3 a estar organizados en un modelo dimensional en Redshift, listos para ser consultados. Airflow garantiza que este proceso se ejecute de forma confiable cada hora, con manejo de errores y validaciones de calidad integradas.

## Conclusiones

- La automatización con Airflow elimina la intervención manual y asegura que los datos estén siempre actualizados para el equipo de analítica.
- El uso de operadores personalizados hace que el pipeline sea modular y reutilizable. Si Sparkify necesita agregar nuevas fuentes de datos o tablas, basta con crear nuevas tareas sin modificar la lógica existente.
- Las validaciones de calidad al final del proceso son fundamentales para detectar problemas tempranamente y evitar que datos incompletos lleguen a los reportes de negocio.
- El modelo de estrella en Redshift ofrece un balance entre simplicidad y rendimiento, permitiendo consultas analíticas rápidas sin necesidad de joins complejos.
- La combinación de S3, Redshift y Airflow demuestra ser una arquitectura sólida y escalable para pipelines de datos en la nube de AWS.
