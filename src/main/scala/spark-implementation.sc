/**
  * R's dplyr operations translated in Spark Scala 2.3.1
  *
  * this is a scala implementation of the basic operations provided by dplyr which is
  * considered the best data munging package in R
  */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// start a spark session
val sparkSession = SparkSession.builder
  .master("local")
  .appName("spark session example")
  .getOrCreate()

// this will enable $"columNname" syntax equivalent to col("columnName")
import sparkSession.implicits._

// read csv
val df = sparkSession.read
  .option("header", "true")
  .csv("src/main/data/flights.csv")
df.show()

// R's summary() in spark
df.describe().show()

/**
  * dplyr::select => DataFrame.select
  * select columns you want by specifying column name(s)
  *
  * for single column: val single_col = df.select("year")
  * Multiple columns require specifying in a Seq(List)
  */
val multi_cols = df.select(Seq("year", "month", "day", "carrier").
  map(c => col(c)): _*)
multi_cols.show()

/**
  * Let's first see all the column types, equivalent to R's str()
  * DataFrame.printSchema()
  */
multi_cols.printSchema()

/**
  * convert type, from string to numeric with condition
  */

val cols_converted = multi_cols.columns.map(x => {
  if (!x.contains("carrier")) col(x).cast(IntegerType)
  else col(x)
})

val my_cols = multi_cols.select(cols_converted: _*)
my_cols.show()

/**
  * see the range of "month" column
  */
my_cols.agg(min(col("month")), max(col("month"))).show()

/**
  * dplyr::filter => DataFrame.filter
  * e.g. only month > 6 and carrier != "AA"
  */
val filtered = my_cols.filter(col("month") > 6 && col("carrier") =!= "AA")
filtered.show()

/**
  * dplyr::arrange => DataFrame.sort/orderBy
  * sort multi columns ascending and descending the same time
  */

val sorted = filtered.sort(asc("day"), desc("carrier"))

// alternatively you can use orderBy
val sorted_2 = filtered.orderBy($"day".asc, $"carrier".desc)

/**
  * dplyr::mutate  => DataFrame.withColumn()
  * here I mutate one column with user defined functions: convert mm to short month string (3 -> Mar)
  */

// import java util lib for this DateFormat operations
import java.text.DateFormatSymbols

// define my conversion function
val to_month_string: (Int => String) = (arg: Int) => {
  new DateFormatSymbols().getShortMonths()(arg - 1)
}

// need to call sparkSession.udf() to convert function to work in Spark(distributed)
val sqlfunc = udf(to_month_string)
val mm_to_string = my_cols.withColumn("month", sqlfunc(col("month")))
mm_to_string.show()

/**
  * dplyr::group_by %>% summarise(n=n()) %>% arrange(desc(n))
  * DataFrame.groupBy.count.orderBy
  * group by and summarise count
  */

val summary = mm_to_string.groupBy($"carrier").count().orderBy($"count".desc)
summary.show()
