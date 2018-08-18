import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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
convert type, from string to numeric with condition
*/

val cols_converted = multi_cols.columns.map(x => {
  if (!x.contains("carrier")) col(x).cast(IntegerType)
  else col(x)
})

val my_cols = multi_cols.select(cols_converted:_*)
my_cols.show()

/*
see the range of "month" column
 */
my_cols.agg(min(col("month")), max(col("month"))).show()

/*
 dplyr::filter
 only month > 6 and carrier != "AA"
 */

val filtered = my_cols.filter(col("month") > 6 && col("carrier") =!= "AA")
filtered.show()

