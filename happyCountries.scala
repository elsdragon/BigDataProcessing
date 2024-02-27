// Databricks notebook source
 // Notebook estudio sobre la felicidad en el mundo


 // Importar los archivos csv como un DataFrame

 
 // Archivo world_happiness_report_2021.csv

 val dfHappy21 = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/FileStore/big-data-processing-2023/world_happiness_report_2021.csv")
 dfHappy21.printSchema
   


// COMMAND ----------

// Numero de registros del DF dfHappy21
dfHappy21.count()

// COMMAND ----------

// Cargar el archivo world_happiness_report.csv

 val dfHappyYears = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/FileStore/big-data-processing-2023/world_happiness_report.csv")
 dfHappyYears.printSchema
 

// COMMAND ----------

// Numero de registros del DF dfHappyYears
dfHappyYears.count()

// COMMAND ----------

// Mostrar que se han cargado correctamente dfHappy21
 dfHappy21.head(5)
 dfHappy21.tail(5)

// COMMAND ----------

// Mostrar que se han cargado correctamente dfHappyYears
 dfHappyYears.head(5)
 dfHappyYears.tail(5)

// COMMAND ----------

// Importacion de librerias
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.expressions.Window


// COMMAND ----------

// ¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score” mayor número más feliz es el país)
// Agrupamos por el nombre del país, buscamos el máximo valor de "Ladder score", ordenamos estos valores de forma descencente y nos quedamos con un valor (el primero) que será el más alto

val maxLadderScore21 = dfHappy21.groupBy("Country name")
  .agg(max(col("Ladder score"))as ("Max Ladder score"))
  .orderBy(col("Max Ladder score").desc)
  .limit(1)

maxLadderScore21.show
    

// COMMAND ----------

// ¿Cuál es el país más “feliz” del 2021 por continente según la data?

// Agrupar por "Regional indicator", seleccionar el valor máximo de Ladder Score y seleccionamos el primer valor de "Country name"

val maxScoreByRegion = dfHappy21.groupBy("Regional indicator").agg(
  max("Ladder score").as("Max_ranking"),
  first("Country name").as ("Pais"))

maxScoreByRegion.show()


// COMMAND ----------

// ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

// Ordenar el DF "dfHappyYears" de forma ascendente por "year" y "Life Ladder"
val sortedHappyYears= dfHappyYears.orderBy(col("year"), col("Life Ladder").desc)

// Seleccionar el primer país en el ranking para cada año.
val firstCountryByYear = sortedHappyYears.groupBy("year")
      .agg(
        first("Country name").as ("Country"),
        max("Life Ladder").as("MaxLifeLadder")
        )

// Mostrar el DataFrame resultante
firstCountryByYear.show()
    

// COMMAND ----------


// Actualizamos el DF con el valor del año 2021 obtenido anteriormente
val add2021 = Seq(
      (2021, "Finland", 7.842),
      )

val newDataDF = spark.createDataFrame(add2021)
    .toDF("year", "Country", "MaxLifeLeader") 

  
val updatedFirstCountry = firstCountryByYear.union(newDataDF)

// DataFrame actualizado
updatedFirstCountry.show()

// COMMAND ----------

// Agrupamos el DF actualizado por Pais, hacmos un conteo de las veces que sale cada país, ordenamos de forma descendente y seleccionamos 2 los dos primeros ya que hay dos valores que están empatados
val mostTopCountry = updatedFirstCountry.groupBy("Country")
      .agg(count("Country").as ("countPositionTop"))
      .orderBy(col("countPositionTop").desc)
      .limit(2)

mostTopCountry.show()


// COMMAND ----------


// ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?

// Creamos un DF filtrando por año 2020
val dfHappy2020= dfHappyYears.filter(col("year") === 2020)

// Calcular el ranking de felicidad para el año 2020

val happyRank2020 = dfHappy2020
      .orderBy(col("Life Ladder").desc)
      .select("Country name", "Life Ladder")
      .withColumn("Ranking", row_number().over(Window.orderBy(col("Life Ladder").desc)))

// happyRank2020.show()

// Calculamos el maximo valor de GDP del año 2020

val maxGDP2020 = dfHappyYears.filter(col("year") === 2020)
      .orderBy(col("Log GDP per capita").desc)
      .first()
// Extraemos el nombre del país al que corresponde ese valor
val countryMaxGDP = maxGDP2020.getAs[String]("Country name")

// Buscamos en el ranking del 2020 el nombre del pais y devolvemos el puesto en el ranking
val happinessRankMaxGDP = happyRank2020.filter(col("Country name") === countryMaxGDP)
      .select("Country name", "Ranking")
   
happinessRankMaxGDP.show()
   

// COMMAND ----------

// ¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?

// Contamos los registros del DF del 2020
dfHappy2020.count()

// Eliminamos los valores nulos en Log GDP per capita
val dfHappy2020Clean = dfHappy2020.na.drop("any", Seq("Log GDP per capita"))

// Contamos los registros del DF limpio
dfHappy2020Clean.count()

// COMMAND ----------

// Eliminamos posibles nulos en datos 2021
dfHappy21.count()
val dfHappy2021Clean = dfHappy21.na.drop("any", Seq("Logged GDP per capita"))
dfHappy2021Clean.count()

// COMMAND ----------

// Calculamos la media de la columna GDP y guardamos el valor
val average2020 = dfHappy2020Clean.agg(avg("Log GDP per capita")).first().getAs[Double](0)
val average2021 = dfHappy2021Clean.agg(avg("Logged GDP per capita")).first().getAs[Double](0)

// Imprimir los resultados de las medias
println(s"La media GDP en 2020 es: $average2020")
println(s"La media GDP en 2021 es: $average2021")

// Calcular el % de variación
val porcentajeAverage20_19 = ((average2021 - average2020) / average2020) * 100

// Imprimir el resultado
println(s"El porcentaje de variación de la media GDP es: $porcentajeAverage20_19%")

// Imprimir si aumentó o disminuyó.

if (porcentajeAverage20_19 > 0) println("Ha aumentado la media de GDP.")
else if (porcentajeAverage20_19 < 0) println("Ha disminuido la media de GDP.")
else println("No ha existido variación")


// COMMAND ----------

// ¿Cuál es el país con mayor expectativa de vida (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia en ese indicador en el 2019?

// Agrupamos el DF dfHappyYears por país, seleccionamos el valor máximo de "Healthy life expectancy at birth", ordenamos por ese valor de forma descendente y nos quedamos con el primer valor (el más alto)
val maxHealthyLife = dfHappyYears.groupBy("Country name")
  .agg(max(col("Healthy life expectancy at birth"))as ("Max Healthy Life"))
  .orderBy(col("Max Healthy Life").desc)
  .limit(1)

maxHealthyLife.show()

// Seleccionamos en una variable el nombre del país anterior
val countryMaxHealthy = maxHealthyLife.first().getAs[String]("Country name")

// Filtramos el DF dfHappy2019 por el nombre anterior y seleccionamos las columnas de nombre de país y Healthy life.

val valueCountry2019 = dfHappy2019.filter(col("Country name")=== countryMaxHealthy)
  .select("Country name", "Healthy life expectancy at birth").show
  
