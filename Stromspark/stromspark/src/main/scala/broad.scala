/**
  * Created by VenkatNag on 11/28/2017.
  */
object broad {

 def get(x: org.apache.spark.broadcast.Broadcast[Map[String,Any]]) =
   {
    x.value
   }

}
