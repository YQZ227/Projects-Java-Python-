import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(x => x.split(",", -1).drop(1)).filter(y => y != "").count
  
    sc.parallelize(Seq(output)).coalesce(1).saveAsTextFile(args(1))
  }
}
