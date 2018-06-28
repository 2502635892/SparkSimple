import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object NodePairWise {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("nodePairWise to List").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/home/frank/Desktop/com-amazon.ungraph.txt")
    val r1 = data.map(x=>x.split("\t"))
    val r2 = r1.map(x=>(x(0).toLong,x(1).toLong))
    val r3 = r2.groupByKey().map(x=>(x._1,x._2.toList))
    r3.saveAsTextFile("/home/frank/Desktop/nodeList")
  }

}
