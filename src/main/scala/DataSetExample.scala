/**
  * Created by ALINA on 27.09.2017.
  */


import org.apache.spark.sql.SparkSession

object DataSetExample {

  def main(args: Array[String]): Unit = {

    val inputFile = args(0);

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-dataset-basic")
      .master("local[*]")
      .getOrCreate();

    //Read json file to DF
    val passengers = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(inputFile)
      .as[Passenger]

    passengers.show(100)
  }
}