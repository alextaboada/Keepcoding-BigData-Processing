// Databricks notebook source
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// MAGIC %md
// MAGIC ### Práctica Data Processing en Scala
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC El proceso para contestar las preguntas tratadas en el enunciado requiere un paso previo, que es convertir los dos archivos csv, uno con datos anteriores a 2021 y otro exclusivamente del 2021, a un dataFrame maestro con los datos combinados. Como los dos csv tienen los nombres de los campos diferentes, debemos tratarlos para unificar nombres y posteriormente unirlos.
// MAGIC
// MAGIC Otro paso que también tenemos que hacer es identificar los campos necesarios para responder a cada pregunta. Estos campos son(en csv global y csv de 2021, en este orden):
// MAGIC 1. Pregunta 1 
// MAGIC     - CSV Global: "Country name" "Year" "Life Ladder" 
// MAGIC     - CSV 2021: "Country name" "Year"(que no existe y deberemos crearlo) "Ladder Score"
// MAGIC 2. Pregunta 2 
// MAGIC     - CSV Global: "Country name" "Year" "Life Ladder" 
// MAGIC     - CSV 2021: "Country Name" "Year"(que no existe y deberemos crearlo) "Ladder Score"
// MAGIC 3. Pregunta 3
// MAGIC     - CSV Global: "Country name" "Year" "Life Ladder"
// MAGIC     - CSV 2021: "Country Name" "Year"(que no existe y deberemos crearlo) "Ladder Score"
// MAGIC 4. Pregunta 4
// MAGIC     - CSV Global: "Country Name" "Year" "Log GDP per capita"
// MAGIC     - CSV 2021: "Country Name" "Year"(que no existe y deberemos crearlo) "Logged GDP per capita"
// MAGIC 5. Pregunta 5
// MAGIC     - CSV Global: "Year" "Log GDP per capita"
// MAGIC     - CSV 2021: "Year"(que no existe y deberemos crearlo)"Logged GDP per capita"
// MAGIC 6. Pregunta 6
// MAGIC     - CSV Global: "Country Name" "Year" "Healthy life expectancy at birth"
// MAGIC     - CSV 2021: "Country Name" "Year"(que no existe y deberemos crearlo) "Healthy life expectancy"
// MAGIC
// MAGIC Por ello, el dataFrame maestro contará con las siguientes columnas:
// MAGIC - Country
// MAGIC - Year
// MAGIC - Ladder
// MAGIC - GDP
// MAGIC - Life expectancy
// MAGIC - Continent
// MAGIC
// MAGIC Una vez creado el dataFrame, procederemos a responder a cada pregunta
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Filtrado de CSV general

// COMMAND ----------

val df1 = spark.read.option("delimiter", ",").option("header", "true").option("inferSchema", "true").csv("/FileStore/world_happiness_report.csv")
display(df1)

// COMMAND ----------

val filtered_df1 = df1.select("Country name", "Year","Life Ladder","Log GDP per Capita","Healthy life expectancy at birth")
.withColumnRenamed("Country name","Country")
.withColumnRenamed("Life Ladder","Ladder")
.withColumnRenamed("Log GDP per Capita","GDP")
.withColumnRenamed("Healthy life expectancy at birth","Life expectancy")
filtered_df1.show()




// COMMAND ----------

val df2 = spark.read.option("delimiter", ",").option("header", "true").csv("/FileStore//world_happiness_report_2021.csv")
display(df2)

// COMMAND ----------

val filtered_df2 = df2.select("Country name","Regional Indicator","Ladder Score","Logged GDP per capita","Healthy life expectancy")
.withColumnRenamed("Country name","Country")
.withColumn("Year",lit(2021))
.withColumnRenamed("Regional Indicator","Continent")
.withColumnRenamed("Ladder Score","Ladder")
.withColumnRenamed("Logged GDP per capita","GDP")
.withColumnRenamed("Healthy life expectancy","Life expectancy")
display(filtered_df2)

// COMMAND ----------

val master_dataframe = filtered_df1.unionByName(filtered_df2,true)sort(col("Country").asc,col("Year").asc)
display(master_dataframe)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pregunta 1
// MAGIC ¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score” mayor número más feliz es el país)

// COMMAND ----------

display(master_dataframe.filter(col("Year") === 2021).orderBy(desc("Ladder")).limit(1))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pregunta 
// MAGIC ¿Cuál es el país más “feliz” del 2021 por continente según la data?
// MAGIC

// COMMAND ----------

val df2021 = master_dataframe.filter(col("Year") === 2021)
val ventana = Window.partitionBy("Continent").orderBy(desc("Ladder"))
val dfConRanking = df2021.withColumn("Ranking", rank().over(ventana))
val paisesMasFelices = dfConRanking.filter(col("Ranking") === 1)
display(paisesMasFelices)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pregunta 3
// MAGIC ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?

// COMMAND ----------

val df3 = master_dataframe.groupBy("Year").agg(max("Ladder"))
display(df3)
