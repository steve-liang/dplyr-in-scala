/*
Ref:
https://lucianomolinari.com/2016/04/30/reading-and-processing-a-csv-file-with-scala/
 */

import java.time._
import java.time.format.DateTimeFormatter

trait CsvReader{
  def read_csv(): Seq[LineItem]
}


/*
> colnames(nycflights13::flights)
 [1] "year"           "month"          "day"            "dep_time"
 [5] "sched_dep_time" "dep_delay"      "arr_time"       "sched_arr_time"
 [9] "arr_delay"      "carrier"        "flight"         "tailnum"
[13] "origin"         "dest"           "air_time"       "distance"
[17] "hour"           "minute"         "time_hour"
 */
case class LineItem(year: Int, month: Int, day: Int, Dep_time: Int,
                    sched_dep_time: Int, dep_delay: Double, arr_time: Int,
                    sched_arr_time: Int, arr_delay: Double, carrier: String,
                    flight: Int, tailnum: String, origin: String, dest: String,
                    air_time: Double, distance: Double, hour: Double, minute: Double,
                    time_hour: LocalDateTime)

val f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")


class NYCFlightsReader(filePath: String) extends CsvReader {
  override def read_csv(): Seq[LineItem] = {
    for {
      line <- io.Source.fromFile(filePath).getLines().drop(1).toVector
      values = line.split(",").map(_.trim)
    } yield LineItem(values(0).toInt, values(1).toInt, values(2).toInt,
      values(3).toInt, values(4).toInt, values(5).toDouble, values(6).toInt,
      values(7).toInt, values(8).toDouble, values(9), values(10).toInt, values(11),
      values(12), values(13), values(14).toDouble, values(15).toDouble, values(16).toDouble,
      values(17).toDouble, LocalDateTime.from(f.parse(values(18)))
    )
  }
}


val flights = new NYCFlightsReader("/Users/steve/IdeaProjects/dplyr-scala/flights.csv").read_csv()

val s = flights.size