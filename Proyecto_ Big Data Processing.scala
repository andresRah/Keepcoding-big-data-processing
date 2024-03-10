// Databricks notebook source
// DBTITLE 1,Importación de Librerias
//Objetos para definir la metadata
import org.apache.spark.sql.types.{StructType, StructField}

//Importamos los tipos de datos que usaremos
import org.apache.spark.sql.types.{StringType, IntegerType, DoubleType}

//Podemos importar todos los utilitarios con la siguiente sentencia
import org.apache.spark.sql.types._

//Importamos todos los objetos utilitarios dentro de una variable
import org.apache.spark.sql.{functions => f}

//Importamos las librerías para implementar UDFs
import org.apache.spark.sql.functions.udf

// COMMAND ----------

// DBTITLE 1,Lectura Archivo world_happiness_report_2021.csv

val schema1 = StructType(Array(
    StructField("country_name", StringType, true),
    StructField("regional_indicator", StringType, true),
    StructField("ladder_score", DoubleType, true),
    StructField("standard_error_of_ladder_score", DoubleType, true),
    StructField("upperwhisker", DoubleType, true),
    StructField("lowerwhisker", DoubleType, true),
    StructField("logged_gdp_per_capita", DoubleType, true),
    StructField("social_support", DoubleType, true),
    StructField("healthy_life_expectancy", DoubleType, true),
    StructField("freedom_to_make_life_choices", DoubleType, true),
    StructField("generosity", DoubleType, true),
    StructField("perceptions_of_corruption", DoubleType, true),
    StructField("ladder_score_in_dystopia", DoubleType, true),
    StructField("explained_by_log_gdp_per_capita", DoubleType, true),
    StructField("explained_by_social_support", DoubleType, true),
    StructField("explained_by_healthy_life_expectancy", DoubleType, true),
    StructField("explained_by_freedom_to_make_life_choices", DoubleType, true),
    StructField("explained_by_generosity", DoubleType, true),
    StructField("explained_by_perceptions_of_corruption", DoubleType, true),
    StructField("dystopia_residual", DoubleType, true)
))

val dfWorldHappy2021 = spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema1)
    .option("mode", "DROPMALFORMED")
    .load("dbfs:/FileStore/shared_uploads/bigdataprocessing-practica/world_happiness_report_2021.csv")

display(dfWorldHappy2021)


// COMMAND ----------

// DBTITLE 1,Lectura Archivo world_happiness_report.csv

val schema2 = StructType(Array(
    StructField("country_name", StringType, true),
    StructField("year", IntegerType, true),
    StructField("life_ladder", DoubleType, true),
    StructField("log_gdp_per_capita", DoubleType, true),
    StructField("social_support", DoubleType, true),
    StructField("healthy_life_expectancy_at_birth", DoubleType, true),
    StructField("freedom_to_make_life_choices", DoubleType, true),
    StructField("generosity", DoubleType, true),
    StructField("perceptions_of_corruption", DoubleType, true),
    StructField("positive_affect", DoubleType, true),
    StructField("negative_affect", DoubleType, true)
))

val dfWorldHappy = spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema2)
    .option("mode", "DROPMALFORMED")
    .load("dbfs:/FileStore/shared_uploads/bigdataprocessing-practica/world_happiness_report.csv")

display(dfWorldHappy)


// COMMAND ----------

// DBTITLE 1, 1)  ¿Cuál es el país más “feliz” del 2021 según la data?
import org.apache.spark.sql.functions._

val happiestCountry2021 = dfWorldHappy2021.orderBy(desc("ladder_score")).limit(1)

display(happiestCountry2021)

// COMMAND ----------

// DBTITLE 1, 2) ¿Cuál es el país más “feliz” del 2021 por continente según la data?
// Solución 1) Usando Spark SQL
import org.apache.spark.sql.functions._

dfWorldHappy2021.createOrReplaceTempView("world_happiness_2021")

val happiestCountriesByRegionSql = 
spark.sql("""
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY regional_indicator ORDER BY ladder_score DESC) as rank
        FROM world_happiness_2021
    ) ranked
    WHERE ranked.rank = 1
""")

display(happiestCountriesByRegionSql)

// Solución 2) Usando Funciones Spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Particionamos por Continente y ordenamos por ladder_score
val regional_partitions = Window.partitionBy("regional_indicator").orderBy(desc("ladder_score"))

// Se agrega un nuevo campo secuencial al dataset denominado "Rank" indicando el máximo "ladder_score"
val withRank = dfWorldHappy2021.withColumn("rank", row_number().over(regional_partitions))

// Filtramos por el "Rank" más alto o país más feliz en cada continente "rank" === 1
val happiestCountriesByRegion = withRank.filter($"rank" === 1)

display(happiestCountriesByRegion)


// COMMAND ----------

// DBTITLE 1,3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

// Solución 1) Usando Spark SQL

import org.apache.spark.sql.functions._

// Creación de vista temporal
dfWorldHappy.createOrReplaceTempView("world_happiness_report")

val countriesInTopByYearSql = spark.sql("""
    WITH CountriesByRank AS (
        SELECT country_name, year,
               RANK() OVER (PARTITION BY year ORDER BY life_ladder DESC) as rank
        FROM world_happiness_report
    ),
    TopCountries AS (
        SELECT country_name, COUNT(*) as number_of_times_in_top
        FROM CountriesByRank
        WHERE rank = 1
        GROUP BY country_name
    )
    SELECT country_name, number_of_times_in_top
    FROM TopCountries
    ORDER BY number_of_times_in_top DESC, country_name
    LIMIT 1
""")

display(countriesInTopByYearSql)

// Solución 2) Usando Spark Functions

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// Se hacen particiones por "year" y se ordena por 'life_ladder' descendentemente
val partitionsByYear = Window.partitionBy("year").orderBy(desc("life_ladder"))

// Con 'row_number' se setea un rango dentro de cada partición de 'year' por 'life_ladder'
val rankedCountries = dfWorldHappy.withColumn("rank", row_number().over(partitionsByYear))

// Con filter obtenemos solo los países con el rango 1, lo que se traduce en el país con el 'life_ladder' más alto cada año
val topCountriesPerYear = rankedCountries.filter($"rank" === 1)

// Se agrupa por 'country_name' y se cuenta las veces que cada país ha sido el número uno
val topCountries = topCountriesPerYear.groupBy("country_name").count().orderBy(desc("count"))

// Selecciona el país que más veces ha sido el número uno
val mostFrequentCountry = topCountries.limit(1)

display(mostFrequentCountry)


// COMMAND ----------

// DBTITLE 1,4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?

// 1) Solución usando Spark SQL

// Creación de vista temporal
dfWorldHappy.createOrReplaceTempView("world_happiness_report")

val happinessForCountry2020_Sql = spark.sql("""
  WITH CountryWith_MaxGDP2020 AS (
      SELECT country_name, log_gdp_per_capita
      FROM world_happiness_report
      WHERE year = 2020
      ORDER BY log_gdp_per_capita DESC
      LIMIT 1
  ), Happiness_Rank2020 AS (
      SELECT country_name, life_ladder,
            RANK() OVER (ORDER BY life_ladder DESC) as happiness_rank
      FROM world_happiness_report
      WHERE year = 2020
  )
  SELECT m.country_name, h.happiness_rank as position 
  FROM CountryWith_MaxGDP2020 m
  JOIN Happiness_Rank2020 h ON m.country_name = h.country_name
""")

display(happinessForCountry2020_Sql)

// 2) Solución usando funciones Spark

import org.apache.spark.sql.expressions.Window

// País con el mayor GDP per Cápita en 2020
val dfWorldHappy_2020 = dfWorldHappy.filter(col("year") === 2020)
val countryWithMaxGDP_2020 = dfWorldHappy_2020.orderBy(desc("log_gdp_per_capita")).first().getAs[String]("country_name")

// Ranking por life_ladder, se agrega una nueva columna al DF llamada "happiness_rank"
val rankingOfHappiness_2020 = dfWorldHappy_2020.withColumn("happiness_rank", rank().over(Window.orderBy(desc("life_ladder"))))

val countryWithMaxGD = rankingOfHappiness_2020
    .filter(col("country_name") === countryWithMaxGDP_2020)
    .select("country_name", "happiness_rank")
    .first()

val countryName = countryWithMaxGD.getAs[String]("country_name")
val happinessRank = countryWithMaxGD.getAs[Int]("happiness_rank")

println(s"El país con el mayor GDP en 2020 fue $countryName, y su puesto de felicidad fue $happinessRank")


// COMMAND ----------

// DBTITLE 1,5. ¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?
import org.apache.spark.sql.functions._

// GDP promedio 2020
val averageGDP2020 = dfWorldHappy.filter(col("year") === 2020)
                             .agg(avg("log_gdp_per_capita")).first().getDouble(0)

// GDP promedio 2021
val averageGDP2021 = dfWorldHappy2021.select(avg(col("logged_gdp_per_capita")).as("promedio")).first().getDouble(0)


// Calcular la variación del GDP promedio de 2020 a 2021
val percentageChange = ((averageGDP2021 - averageGDP2020) / averageGDP2020) * 100

println("============================================================================")
println("Promedio GDP 2020: " + averageGDP2020)
println("Promedio GDP 2021: " + averageGDP2021)
println("Variación porcentual del GDP (2020 - 2021): " +percentageChange)

val result = if (percentageChange > 0) "Aumento" else "Disminuyo"

println("Se puede concluir que la Variación porcentual del GDP (2020 - 2021): " +result)
println("============================================================================")


// COMMAND ----------

// DBTITLE 1,6. ¿Cuál es el país con mayor expectativa de vida (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia en ese indicador en el 2019?
// 1) Solución usando Spark SQL

// Creación de vista temporal
dfWorldHappy.createOrReplaceTempView("world_happiness_report")

val countryWithMaxHealthyLifeExpectancy_Sql = spark.sql("""
    SELECT country_name, MAX(healthy_life_expectancy_at_birth) as max_expectancy
    FROM world_happiness_report
    GROUP BY country_name
    ORDER BY max_expectancy DESC
    LIMIT 1
""")

// Leemos el country_name de la primera Row
val countryName = countryWithMaxHealthyLifeExpectancy_Sql.collect()(0).getAs[String]("country_name")

val healthyLifeExpectancy2019_Sql = spark.sql(s"""
    SELECT country_name, healthy_life_expectancy_at_birth
    FROM world_happiness_report
    WHERE country_name = '$countryName' AND year = 2019
""")

display(healthyLifeExpectancy2019_Sql)

// 2) Solución usando funciones Spark

import org.apache.spark.sql.functions._

// Traer el máximo valor de "Healthy life expectancy at birth" y el "country"
val maximumExpectancy_Row = dfWorldHappy
    .select("country_name", "healthy_life_expectancy_at_birth")
    .orderBy(desc("healthy_life_expectancy_at_birth"))
    .first()

val maxExpectancyCountry = maximumExpectancy_Row.getAs[String]("country_name")

val expectancy2019 = dfWorldHappy
    .filter(col("country_name") === maxExpectancyCountry && col("year") === 2019)
    .select("healthy_life_expectancy_at_birth")
    .first()
    .getAs[Double]("healthy_life_expectancy_at_birth")

println("==================================================================================")
println("El pais con mayor expectativa de vida al nacer es: " + maxExpectancyCountry)
println("La expectativa de vida para el país " + maxExpectancyCountry + " en 2019 fue de: " + expectancy2019)
println("==================================================================================")

