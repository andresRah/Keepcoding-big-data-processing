# Keepcoding-big-data-processing
Keepcoding Big Data Processing 

# Análisis de la Felicidad Mundial

Este proyecto analiza datos del Informe Mundial de la Felicidad para responder a varias preguntas clave sobre la felicidad, la economía y la salud en diferentes países y a lo largo de los años. El análisis se realiza utilizando un notebook de Scala en la plataforma Databricks.

## Preguntas de Investigación

1. **País más feliz en 2021**: Identificamos el país con el mayor "Ladder Score" en 2021, considerando este valor como un indicador de la felicidad del país. 

2. **País más feliz por continente en 2021**: Analizamos los datos para encontrar el país más feliz en cada continente durante el año 2021.

3. **País que más veces fue el más feliz**: Determinamos cuál país ha ocupado el primer lugar en términos de felicidad más veces a lo largo de todos los años disponibles en el conjunto de datos.

4. **Puesto de felicidad del país con mayor GDP en 2020**: Investigamos qué posición en el ranking de felicidad ocupaba el país con el mayor GDP per cápita en 2020.

5. **Variación porcentual del GDP promedio mundial**: Calculamos cómo varió el GDP promedio a nivel mundial de 2020 a 2021, especificando si hubo un aumento o una disminución.

6. **País con mayor expectativa de vida y su valor en 2019**: Identificamos el país con la mayor expectativa de vida saludable al nacer y proporcionamos el valor específico de este indicador para el año 2019.

## Metodología

Para realizar este análisis, hemos seguido una metodología consistente que implica:

- **Carga de Datos**: Importamos los conjuntos de datos del Informe Mundial de la Felicidad en DataFrames de Spark.
- **Limpieza y Preparación de Datos**: Aseguramos que los datos estén en formatos adecuados y libres de inconsistencias.
- **Análisis Exploratorio**: Realizamos una exploración inicial para entender la distribución y las características clave de los datos.
- **Respuesta a Preguntas de Investigación**: Utilizamos funciones de Spark SQL y Spark DataFrame API para filtrar, ordenar y calcular estadísticas relevantes que responden a nuestras preguntas de investigación.
- **Visualización de Resultados**: Aunque el enfoque principal ha sido el análisis cuantitativo, donde fue posible, visualizamos los resultados para una mejor interpretación.

## Herramientas Utilizadas

- **Databricks**: Plataforma de análisis basada en la nube que proporciona un entorno interactivo para trabajar con notebooks.
- **Apache Spark**: Motor de análisis distribuido que facilita el manejo y procesamiento de grandes conjuntos de datos.
- **Scala**: Lenguaje de programación utilizado para implementar las transformaciones y análisis de datos dentro de los notebooks de Databricks.

## Cómo Usar

Este análisis se encuentra estructurado en un notebook de Databricks. Para replicar o extender el estudio, se recomienda:

1. Importar el notebook a tu propio entorno de Databricks.
2. Asegurarte de tener acceso a los conjuntos de datos del Informe Mundial de la Felicidad y cargarlos en Databricks.
3. Ejecutar las celdas del notebook secuencialmente para comprender el flujo de análisis y obtener los resultados.
