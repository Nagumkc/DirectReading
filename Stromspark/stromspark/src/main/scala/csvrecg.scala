import java.util

import FeatureExtraction.oneOfK
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.SparkSession
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple}

/**
  * Created by VenkatNag on 11/28/2017.
  */
class csvrecg extends BaseRichBolt {
  var _collector: OutputCollector = null

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("sent","pred"))

  }
  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    _collector = outputCollector
  }
  override def execute(tuple: Tuple): Unit = {
    if (strrecgspout.recg.equalsIgnoreCase("True")) {
      val sc = SparkContext.getOrCreate()
      val spark = SparkSession.builder.appName("iHearWOrd2Vec").master("local[*]").getOrCreate()
      val test = tuple.getStringByField("test")
      val model = NaiveBayesModel.load(sc, "data/nb")
      val testdata=sc.parallelize(List(test))
      val data = testdata.map(_.split(","))
        .map(p => (p(0), p(1).toInt, p(2).toInt, p(3), p(4).toInt))
      val labelCol = "play"
      //  val featureCols = data.schema.fields.map(c => c.name).filter(c => c != labelCol)
      val categoricalCols = Array("outlook", "temperature", "humidity", "wind")

      //2. Feature extraction
      val (categoryIndexes, featuresRDD) = oneOfK(data, categoricalCols, categoricalCols)
      val labelsRDD =data.map(record=>record._5.asInstanceOf[Int].toDouble)
      val dataLP = labelsRDD.zip(featuresRDD).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
      dataLP.cache()
      val labelAndpreds = dataLP.map(p => (p.label,model.predict(p.features)))
       labelAndpreds.foreach(println)
    }
  }
}
