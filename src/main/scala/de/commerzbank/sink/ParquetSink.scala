package de.commerzbank.sink

class ParquetSink {

}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext


object BXMParquet extends App {

  val conf = new SparkConf().setAppName("Parquet").setMaster("local[*]")
  val sc = new SparkContext(conf)

//  val data = Array(1, 2, 3, 4, 5)
//  val dataRDD = sc.parallelize(data)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val df = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
  df.show()
  df.write.parquet("data.parquet")

}
