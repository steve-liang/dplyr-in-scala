# dplyr-in-scala

> This is a self-learning guide of scala implementation of the basic operations provided by dplyr which is
  considered the best data munging package in R

### Preparation  
Before anything, we need to import the relevant references for spark to work

```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
```

Now we can start a spark session and connect to local node
```
// start a spark session
val sparkSession = SparkSession.builder
  .master("local")
  .appName("dplyr-in-scala")
  .getOrCreate()
```

Additionally for convenience, we want to import (Scala-specific) Implicit methods available for converting common Scala objects into DataFrames.
```
// this will enable $"columNname" syntax equivalent to col("columnName")
import sparkSession.implicits._
```

We can load some data in to play with. Commonly csv file is easy to start with.
```
// read csv
val df = sparkSession.read
  .option("header", "true")
  .csv("src/main/data/flights.csv")
df.show()
```

In R, typically we can generate quick data summary (min/mean/max/std) by calling summary() function. This is super simple in Spark too by calling describe().
If you've used Python's pandas, you will be not surprised why they pick describe instead of summary.
```
// R's summary() in spark
df.describe().show()
```

### Translate dplyr operations

#### select
```
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
```

by default csv file loaded are all in String type. For data analysis, it will be make more sense to convert String type to its original types.  

in R we can call str() to check types of each columns. This can be done similarly in Spark by calling printSchema()
```
/**
  * Let's first see all the column types, equivalent to R's str()
  * DataFrame.printSchema()
  */
multi_cols.printSchema()
```

Year/Month/Day columns in nycflight13 dataset should be converted to Integer. This can be acheived by calling
.cast(IntegerType)
```
/**
  * convert type, from string to numeric with condition
  */

val cols_converted = multi_cols.columns.map(x => {
  if (!x.contains("carrier")) col(x).cast(IntegerType)
  else col(x)
})

val my_cols = multi_cols.select(cols_converted: _*)
my_cols.show()
```

Now month column has been converted to Integer. Easily we can get the range of the months

```
/**
  * see the range of "month" column
  */
my_cols.agg(min(col("month")), max(col("month"))).show()
```

#### filter

Now let's put on some filter condition to subset the data, e.g. select only carrier = AA and month > 6
in Spark this is the same function name as in dplyr - filter
```
/**
  * dplyr::filter => DataFrame.filter
  * e.g. only month > 6 and carrier != "AA"
  */
val filtered = my_cols.filter(col("month") > 6 && col("carrier") =!= "AA")
filtered.show()
```

### arrange

If you want to sort one of the columns, we can either use sort or orderBy.
Moreover, you can specify sort order ascending/descending (asc(), desc()).

```
/**
  * dplyr::arrange => DataFrame.sort/orderBy
  * sort multi columns ascending and descending the same time
  */

val sorted = filtered.sort(asc("day"), desc("carrier"))

// alternatively you can use orderBy
val sorted_2 = filtered.orderBy($"day".asc, $"carrier".desc)

```

#### mutate

Let's do something a bit more complex. Apply some function to mutate one of the columns.
This will involve in 3-step process. 
* First define a user defined function 
* call sparkSession.udf() to convert the scala function to distributed
* apply the distributed function to column by calling .withColumn()

```
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
```

#### summarise
One of the most frequently used process in dplyr is group_by -> summarise can be easily replicated in spark
this can be done in a chained procedure fashion.
Syntax is really similar to dplyr 
```
/**
  * dplyr::group_by %>% summarise(n=n()) %>% arrange(desc(n))
  * DataFrame.groupBy.count.orderBy
  * group by and summarise count
  */

val summary = mm_to_string.groupBy($"carrier").count().orderBy($"count".desc)
summary.show()
```