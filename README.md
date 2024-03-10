# Keepcoding-big-data-processing
Keepcoding Big Data Processing 

# Análisis de la Felicidad Mundial

Este proyecto analiza datos del Informe Mundial de la Felicidad para responder a varias preguntas clave sobre la felicidad en diferentes países y a lo largo de los años. El análisis se realiza utilizando un notebook de Scala en la plataforma Databricks. https://www.databricks.com/

## Preguntas de Investigación

1. **País más feliz en 2021**: Identificamos el país con el mayor "Ladder Score" en 2021, considerando este valor como un indicador de la felicidad del país. 

2. **País más feliz por continente en 2021**: Analizamos los datos para encontrar el país más feliz en cada continente durante el año 2021.

3. **País que más veces fue el más feliz**: Determinamos cuál país ha ocupado el primer lugar en términos de felicidad más veces a lo largo de todos los años disponibles en el conjunto de datos.

4. **Puesto de felicidad del país con mayor GDP en 2020**: Investigamos qué posición en el ranking de felicidad ocupaba el país con el mayor GDP per cápita en 2020.

5. **Variación porcentual del GDP promedio mundial**: Calculamos cómo varió el GDP promedio a nivel mundial de 2020 a 2021, especificando si hubo un aumento o una disminución.

La variación porcentual del GDP promedio mundial entre 2020 y 2021, para este cálculo primero necesitamos calcular el GDP promedio para cada uno de esos años y luego aplicar la fórmula de variación porcentual. La fórmula de variación porcentual se define como:

![image](https://github.com/andresRah/Keepcoding-big-data-processing/assets/10521199/d1b6956e-5714-453b-a8da-dd3f76f2f339)

6. **País con mayor expectativa de vida y su valor en 2019**: Identificamos el país con la mayor expectativa de vida saludable al nacer y proporcionamos el valor específico de este indicador para el año 2019.

## Metodología

Para realizar este análisis, implica:

- **Carga de Datos**: Importar los conjuntos de datos del Informe Mundial de la Felicidad en DataFrames de Spark.
- **Limpieza y Preparación de Datos**: Asegurar que los datos cuenten con los formatos adecuados, tipos de datos y libres de inconsistencias.
- **Análisis Exploratorio**: Realizar una exploración inicial para entender la distribución y las características de los datos.
- **Respuesta a Preguntas de Investigación**: Utilizar funciones de Spark SQL y Spark DataFrame API para filtrar, ordenar y calcular estadísticas que responden las preguntas planteadas.
- **Visualización de Resultados**: Mostrar la información por medio del comando display().

## Herramientas Utilizadas

- **Databricks**: Plataforma de análisis basada en la nube que proporciona un entorno interactivo para trabajar con notebooks de Scala y Python.
- **Apache Spark**: Motor de análisis distribuido que facilita el manejo y procesamiento de grandes conjuntos de datos.
- **Scala**: Lenguaje de programación utilizado para implementar las transformaciones y análisis de datos dentro de los notebooks de Databricks.

## Cómo Usar

Este análisis se encuentra estructurado en un notebook de Databricks. Para replicar o extender el estudio, se recomienda:

1. Importar el notebook a un entorno de Databricks.
2. Cargar los archivos "world-happiness-report-2021.csv" y "world-happiness-report.csv" en Databricks y asegurarse de tener el modo DBFS activo.
3. Ejecutar las celdas del notebook secuencialmente para comprender el flujo de análisis e ir obteniendo los resultados.
