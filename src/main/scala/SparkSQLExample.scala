import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLExample {

  def main(args: Array[String]) {

    //Create SparkContext
    val conf = new SparkConf()
    conf.setAppName("spark-sql-example")
    conf.setMaster("local[*]")
    conf.set("spark.executor.memory", "1g")
    conf.set("spark.driver.memory", "1g")

    val sc = new SparkContext(conf)

    // Create SQLcontext
    val sqlContext = new SQLContext(sc)

    // Create the DataFrame
    val df = sqlContext.read.json("src/main/resources/cars.json")

    // Show the Data
    df.show()
  }
}
