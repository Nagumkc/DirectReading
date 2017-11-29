import java.io.{FileOutputStream, FileWriter, PrintStream}
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, RandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}

import scala.runtime.Nothing$

/**
  * Created by VenkatNag on 10/14/2017.
  */
class recogbolt extends BaseRichBolt
{
  var _collector: OutputCollector = null

override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
  outputFieldsDeclarer.declare(new Fields("sent","pred"))

}
override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
  _collector = outputCollector
}
override def execute(tuple: Tuple): Unit = {
  // val model=tuple.getStringByField("model")
  //val sc=tuple.getValueByField("context");
  //val spark=tuple.getValueByField("spark");
  if (recgspout.rec.equalsIgnoreCase("True")) {
    val sc = SparkContext.getOrCreate()
    val spark = SparkSession.builder.appName("iHearWOrd2Vec").master("local[*]").getOrCreate()
    // val s = tuple.getStringByField("model")
    val test = tuple.getStringByField("test")
//    val size=tuple.getStringByField("model")
    val size=spout.size
  //  println(test)
    val model = DecisionTreeModel.load(sc, "data/decision")
    // val test = "E:\\UMKC\\Sum_May\\KDM\\Week 5\\Yahoo-Question-testdata.csv"
    val (testvector, testdata, testvoc) = preprocess(sc, test,size)
    val tdata=testdata.zip(testvector)
    val testfeaturevector=tdata.map(f=>{new LabeledPoint(f._1._1.toString.toDouble,f._2)})
    val test_vector=tdata.map(f=>{f._2})
    val predict=testfeaturevector.map(f=>(model.predict(f.features),f.label))
    val x=for((e,a) <- (predict zip testdata)) yield (a._2,e._1)
    val y=x.map(f=>(f._1,
      if(f._2==1.0) "Business&Finance"
      else if(f._2==2.0) "Computers&Internet"
      else if(f._2==3.0) "Entertainment&Music"
      else if(f._2==4.0) "Family&Relationships"
      else if(f._2==5.0) "Education&Reference"
      else if(f._2==6.0) "Health"
      else if(f._2==7.0) "Science&Mathematics"))
     y.foreach(println)

   /* val fos = new FileOutputStream("data/result", true)
val topic_output = new PrintStream(fos)
    topic_output.println(y.take(1))
    topic_output.flush()
    topic_output.close()
    fos.close()*/
  //    println(y.collect())
    // Confusion matrix
    /* topic_output.println("Confusion matrix:")
  topic_output.println(metrics.confusionMatrix)

  topic_output.println("Accuracy: " + accuracy)
  topic_output.flush()
  topic_output.close()
  println(accuracy)*/


    //y.map(f=>{_collector.emit(new Values(f._1.toString,f._2.toString))})


  }
}

  private def preprocess(sc: SparkContext,paths: String,size:Int): (RDD[Vector], RDD[(String,String)], Long) = {

    //Reading Stop Words
    val stopWords=sc.textFile("data/stopwords.txt").collect()
    val stopWordsBroadCast=sc.broadcast(stopWords)

    val df1 = sc.parallelize(List(paths))
    val df=df1.map(f => f.split(",")).map(f=>{
      //   val lemma=CoreNLP.returnLemma(f(1))
      val splitString = f(1).split(" ")
      (f(0),splitString)
    })


    val stopWordRemovedDF=df.map(f=>{
      //Filtered numeric and special characters out
      val filteredF=f._2.map(_.replaceAll("[^a-zA-Z]",""))
        //Filter out the Stop Words
        .filter(ff=>{
        if(stopWordsBroadCast.value.contains(ff.toLowerCase))
          false
        else
          true
      })
      (f._1,filteredF)
    })

    val data=stopWordRemovedDF.map(f=>{(f._1,f._2.mkString(" "))})
    val dfseq=stopWordRemovedDF.map(_._2.toSeq)

    //Creating an object of HashingTF Class
    val hashingTF = new HashingTF(size)  // VectorSize as the Size of the Vocab

    //Creating Term Frequency of the document
    val tf = hashingTF.transform(dfseq)
    tf.cache()

    val idf = new IDF().fit(tf)
    //Creating Inverse Document Frequency
    val tfidf1 = idf.transform(tf)
    tfidf1.cache()



    val dff= stopWordRemovedDF.flatMap(f=>f._2)
    val vocab=dff.distinct().collect()
    (tfidf1, data, dff.count()) // Vector, Data, total token count
  }

}
