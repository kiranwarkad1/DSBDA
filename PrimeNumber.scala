import org.apache.spark.sql.SparkSession

object PrimeNumber {
  def main(args: Array[String]): Unit = {

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("PrimeNumberChecker")
      .master("local[*]") // Run locally with all cores
      .getOrCreate()

    val sc = spark.sparkContext

    // Function to check if a number is prime
    def isPrime(n: Int): Boolean = {
      if (n <= 1) false
      else if (n == 2) true
      else !(2 to math.sqrt(n).toInt).exists(x => n % x == 0)
    }

    // Range of numbers to check
    val numberRange = 1 to 100

    // Parallelize the range using Spark
    val numbersRDD = sc.parallelize(numberRange)

    // Filter prime numbers
    val primeNumbers = numbersRDD.filter(isPrime)

    // Collect and print results
    println("Prime Numbers:")
    primeNumbers.collect().foreach(println)

    // Stop Spark session
    spark.stop()
  }
}
