import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ListBuffer}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(x => {
      var ratings = x.split(",");
      var max = 0;
      var name = rating(0);
      var audiunce = ListBuffer.empty[Int];

      for (i <- 1 until rating.length) {
        var tempRating = ratings(i);
        if (tempRating != "") {
          var rating = tempRating.toInt;
          if (rating == max) {
            audiunce += i;
          }
          if (rating > max) {
            audiunce.clear;
            max = rating;
            audiunce += i;
          }
        }
      } name + "," + audiunce.mkString(",");
    })
    
    output.saveAsTextFile(args(1))
  }
}
