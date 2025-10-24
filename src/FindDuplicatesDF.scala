package SparkExample.Interview
package src

object FindDuplicatesDF {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("find duplicates in DF").getOrCreate()
    val data1 = Seq(10, 20, 20, 30, 30, 30, 40)

    val df1 = data1.toDF("value")
    val dupes = df1.groupBy("value").count.filter("count > 1")
    dupes.show() // i am getting errors
  }
}
