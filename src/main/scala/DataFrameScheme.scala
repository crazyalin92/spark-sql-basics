/**
  * Created by ALINA on 29.09.2017.
  */


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, StringType}

object DataFrameScheme {

  def main(args: Array[String]): Unit = {

    val inputFile = args(0);

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-read-csv")
      .master("local[*]")
      .getOrCreate();

    //Define the scheme of the data
    var titanicSchema = StructType(Array(
      StructField("PassengerId", IntegerType, true),
      StructField("Survived", IntegerType, true),
      StructField("Pclass", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Sex", StringType, true),
      StructField("Age", DoubleType, true),
      StructField("SibSp", IntegerType, true),
      StructField("Parch", IntegerType, true),
      StructField("Ticket", StringType, true),
      StructField("Fare", DoubleType, true),
      StructField("Cabin", StringType, true),
      StructField("Embarked", StringType, true)))


    //Read CSV file to DF and define scheme on the fly
    val passengers = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(titanicSchema)
      .csv(inputFile)

    passengers.show(100)
    passengers.printSchema()
  }
}
