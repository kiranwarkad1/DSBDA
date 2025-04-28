import org.apache.spark.sql.SparkSession

object NumberCheck {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
                            .appName("Number Check")
                            .master("local[*]")  // run locally using all available cores
                            .getOrCreate()

    // Define a list of numbers
    val numbers = spark.sparkContext.parallelize(Seq(10, -5, 0, 20, -15, 50))

    // Define a function to check if the number is positive, negative, or zero
    def checkNumber(number: Int): String = {
      if (number > 0) {
        "Positive"
      } else if (number < 0) {
        "Negative"
      } else {
        "Zero"
      }
    }

    // Apply the function to each number using map
    val results = numbers.map(checkNumber)

    // Collect the results and print them
    results.collect().foreach(println)

    // Stop the Spark session
    spark.stop()
  }
}
