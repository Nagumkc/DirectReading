import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import FeatureExtraction.oneOfK
import com.sun.prism.impl.Disposer.Record
/**
  * Created by VenkatNag on 11/14/2017.
  */
class csvdatamodel extends BaseRichBolt  {
 var _collector: OutputCollector = null
  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("model"))

  }

  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    _collector = outputCollector
  }

  override def execute(tuple: Tuple) : Unit  = {
    val str = tuple.getString(0);
    if (str != null | str != " ") {

      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      System.setProperty("hadoop.home.dir", "E:\\UMKC\\Sum_May\\KDM\\winutils")

      if (str.length == 0) {
        println("Must specify training files.")
        sys.exit(1)
      }

      val conf = new SparkConf().setAppName(s"KMeansExample with ").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
      val sc = new SparkContext(conf)
      val spark = SparkSession.builder.appName("iHearWOrd2Vec").master("local[*]").getOrCreate()
      //1. Data preparation
      val file = str
     /* val data = sc.textFile(file)
      val parsedData = data.map( line => {
        val parts = line.split(',')
        val features = parts.tail.take(parts.length - 2)
        LabeledPoint(parts(4).toDouble, Vectors.dense(features.map(_.toDouble)))
      }
      )*/

      val data = sc.textFile(file).map(_.split(","))
        .map(p => (p(0), p(1).toInt, p(2).toInt, p(3), p(4).toInt))

      val labelCol = "play"
      //  val featureCols = data.schema.fields.map(c => c.name).filter(c => c != labelCol)
      val categoricalCols = Array("outlook", "temperature", "humidity", "wind")

      //2. Feature extraction
      val (categoryIndexes, featuresRDD) = oneOfK(data, categoricalCols, categoricalCols)
      val labelsRDD =data.map(record=>record._5.asInstanceOf[Int].toDouble)
      val dataLP = labelsRDD.zip(featuresRDD).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
       dataLP.cache()
      //3. Model Fitting
   //   val splits = dataLP.randomSplit(Array(0.7, 0.3))
   //   val (trainingData, testData) = (splits(0), splits(1))
   //   trainingData.cache()
  //    println("rows in training and test data respectively %d, %d".format(trainingData.count, testData.count))
      val model = NaiveBayes.train(dataLP, 1.0)

      model.save(sc,"data/nb")
      broad.get(sc.broadcast(categoryIndexes))
      strrecgspout.recg="True"

      //4. Model Evaluation
   //   val labelAndpreds = testData.map(p => (p.label,model.predict(p.features)))
  //    labelAndpreds.foreach(println)
    //  val accuracy = 1.0 * labelAndpreds.filter(x => x._1 == x._2).count/testData.count

      // println("The accuracy of the model is %2.4f%%".format(accuracy * 100))
    }
  }

}
