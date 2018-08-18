import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
  * Spark Session example
  *
  */

val sparkSession = SparkSession.builder
  .master("local")
  .appName("spark session example")
  .getOrCreate()

val df = sparkSession.read
  .option("header", "true")
  .csv("src/main/data/flights.csv")

df.show()

/*
 dplyr::select

 val single_col = df.select("year")

  */
val multi_cols = df.select(Seq("year", "month", "day", "carrier").map(c => col(c)): _*)

multi_cols.show()

/*
Let's first see all the column types, equivalent to R's str()
printSchema()
 */

multi_cols.printSchema()

/*
convert type, from string to numeric
 */

val reformat_cols = multi_cols.select(multi_cols.columns.map(c => col(c).cast("Integer")):_*)
/*
  dplyr::filter
 */

reformat_cols.printSchema()