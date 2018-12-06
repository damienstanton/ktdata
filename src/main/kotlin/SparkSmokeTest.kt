package codes.damien.ktdata

import org.apache.spark.sql.SparkSession

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val logFile = "src/main/resources/script.txt"
        val spark = SparkSession
                        .builder()
                        .appName("Simple Application")
                        .config("spark.master", "local")
                        .orCreate
        val logData = spark.read().textFile(logFile).cache()

        val countA = logData.filter { it.contains("a") }.count()
        val countB = logData.filter{ it.contains("b") }.count()

        println("Lines with a: $countA, lines with b: $countB")
        spark.stop()
    }
}
