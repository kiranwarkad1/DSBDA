import org.apache.spark.sql.SparkSession

object Calculator {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Simple Spark Calculator")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    // List of operations: (operation, number1, number2)
    val operations = Seq(
      ("add", 10, 5),
      ("sub", 20, 8),
      ("mul", 4, 3),
      ("div", 16, 4),
      ("div", 10, 0) // Division by zero test
    )

    // Parallelize the operations
    val opsRDD = sc.parallelize(operations)

    // Function to calculate based on operator
    def calculate(op: String, a: Int, b: Int): String = {
      op match {
        case "add" => s"$a + $b = ${a + b}"
        case "sub" => s"$a - $b = ${a - b}"
        case "mul" => s"$a * $b = ${a * b}"
        case "div" =>
          if (b == 0) s"$a / $b = undefined (division by zero)"
          else s"$a / $b = ${a / b}"
        case _     => s"Unknown operation: $op"
      }
    }

    // Apply calculation function
    val results = opsRDD.map { case (op, a, b) => calculate(op, a, b) }

    // Collect and print results
    results.collect().foreach(println)

    // Stop Spark session
    spark.stop()
  }
}
