import org.apache.spark.sql.SparkSession
import kotlin.test.*
import org.junit.Test

class Tests {
    @Test
    fun testSpark() {
        val logFile = "src/main/resources/script.txt"
        val spark = SparkSession
            .builder()
            .appName("Simple Application")
            .config("spark.master", "local")
            .orCreate
        val logData = spark.read().textFile(logFile).cache()

        val countA = logData.filter { it.contains("a") }.count()
        val countB = logData.filter{ it.contains("b") }.count()

        assertEquals(2, countA)
        assertEquals(3, countB)

        spark.stop()
    }
}



