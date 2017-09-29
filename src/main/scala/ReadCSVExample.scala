/**
  * Created by ALINA on 27.09.2017.
  */


import org.apache.spark.sql.SparkSession

object ReadCSVExample {

  def main(args: Array[String]): Unit = {

    val inputFile = args(0);

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-read-csv")
      .master("local[*]")
      .getOrCreate();

    //Read CSV file to DF and define scheme on the fly
    val passengers = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(inputFile)

    passengers.show(100)
    passengers.printSchema()
  }
}
