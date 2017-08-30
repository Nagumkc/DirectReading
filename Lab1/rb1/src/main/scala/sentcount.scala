import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by VenkatNag on 8/24/2017.
  */
object sentcount {

  def main(args:Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\UMKC\\Sum_May\\KDM\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkTransformation").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val v=sc.textFile("E:\\UMKC\\Fall_Aug\\Direct Reading\\week 1\\text.txt")

    val y=v.flatMap(f=>{f.split('.')}).map(w=>(w,1))

    val o=y.reduceByKey(_+_)

    val x=o.sortByKey()

    x.collect().foreach(println)
  }
}
