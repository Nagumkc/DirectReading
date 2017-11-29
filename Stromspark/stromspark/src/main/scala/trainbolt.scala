import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

/**
  * Created by VenkatNag on 11/4/2017.
  */
class trainbolt extends BaseRichBolt {
  var _collector: OutputCollector = null

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("model"))

  }
  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    _collector = outputCollector
  }
  override def execute(tuple: Tuple): Unit = {
    val str = tuple.getString(0);

    if (str != null | str != " ") {
      println(str)

      System.setProperty("hadoop.home.dir", "E:\\UMKC\\Sum_May\\KDM\\winutils")
      val conf = new SparkConf().setAppName(s"KMeansExample with ").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
      val sc = new SparkContext(conf)
      val spark = SparkSession.builder.appName("iHearWOrd2Vec").master("local[*]").getOrCreate()
      val input = str
      // val input="E:\\UMKC\\Sum_May\\KDM\\Week 5\\Yahoo-Questions-traindata.csv"
      //  val test = "E:\\UMKC\\Sum_May\\KDM\\Week 5\\Yahoo-Question-testdata.csv"
      Logger.getRootLogger.setLevel(Level.WARN)
      val data = MLUtils.loadLibSVMFile(sc, input).cache()

      val training = data
      val IMAGE_CATEGORIES = List("1", "2", "3", "4","5","6","7")

      val numClasses = IMAGE_CATEGORIES.length
      val categoricalFeaturesInfo = Map[Int, Int]()
      val impurity = "gini"
      val featureSubSet = "auto"
      val maxDepth = 5
      val maxBins = 32


      val model = DecisionTree.trainClassifier(training, numClasses+1, categoricalFeaturesInfo,
        impurity, maxDepth, maxBins)
      model.save(sc,"data/decision")

      recgspout.rec="True"
      _collector.emit(new Values("done"))
      _collector.ack(tuple)
    }
  }
}
