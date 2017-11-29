/**
 * Created by snudurupati on 4/23/15.
 * Machine Learning helper library used for feature extraction, especially categorical features.
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object FeatureExtraction {

  /*
  Performs 1-of-k encoding on categorical columns and returns an RDD of the feature array
   */

  def oneOfK(data: RDD[(String,Int,Int,String,Int)], categoricalCols: Array[String], featureCols: Array[String]) = {

    //takes a categorical column, performs 1-of-k encoding and returns a Map of category -> index
    def indexColumn(col: String) = {
      if(col=="outlook")
      data.map(r=>r._1).distinct.collect.zipWithIndex.toMap
     else if(col=="temperature")
        data.map(r=>r._2).distinct.collect.zipWithIndex.toMap
     else if(col=="humidity")
        data.map(r=>r._3).distinct.collect.zipWithIndex.toMap
     else if(col=="wind")
        data.map(r=>r._4).distinct.collect.zipWithIndex.toMap
    }

    //encodes each categorical column as a Map of category -> index and return a Map of categorical Maps
    val categoryIndexes = categoricalCols.map(c => (c, indexColumn(c))).toMap

    //replaces categorical features with corresponding indexes and returns a features array
    def reIndexRow(row: Row) = {
      val features = Array.ofDim[Double](row.size)
      for (i <- 0 to row.size - 2) {
        if (categoricalCols.contains(featureCols(i))) {
          features(i)= categoryIndexes(featureCols(i)).asInstanceOf[Map[Any,Int]](row(i)).toDouble
      //    println(features(i))
        }
        else
          features(i) = row(i).asInstanceOf[Int].toDouble
      }
      features
    }

    val featureRDD = data.map(r => reIndexRow(Row(r._1,r._2,r._3,r._4,r._5)))

    (categoryIndexes, featureRDD)
  }

}
