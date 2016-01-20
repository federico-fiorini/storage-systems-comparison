import java.io.File

import org.mongodb.scala.{MongoClient, MongoDatabase, Observer, Completed}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

import org.bson.BSONObject
import org.bson.BasicBSONObject
import com.mongodb.hadoop.{
  MongoInputFormat, MongoOutputFormat,
  BSONFileInputFormat, BSONFileOutputFormat
}

object ImportToMongo {
  def main(args: Array[String]) { 

    val conf = new SparkConf().setAppName("Flights Dataset")
    val sc = new SparkContext(conf)

    cleanMongo()

    val d = new File("dataset")
    val fileList = d.listFiles.filter(_.isFile).toList
    for (csvFile <- fileList) {
      val dataset = sc.textFile(csvFile.getPath())
      saveToMongo(dataset)
    }
  }

  def cleanMongo() {
    val mongoClient: MongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase("flights");
    val collection = database.getCollection("routes");
    collection.drop().subscribe(new Observer[Completed] {

      override def onNext(result: Completed): Unit = {}

      override def onError(e: Throwable): Unit = {}

      override def onComplete(): Unit = {}
    })
  }

  def saveToMongo(dataset: RDD[String]) {
    
    val rows = dataset.map(r => r.split(",")).filter( l => !(l contains "Year") ) // Split values and drop the header
    val tuples = rows.map(row => ( Tuple5(row(0), row(1), row(1), row(16), row(17)), 1 ) )
    val flightsCount = tuples.reduceByKey(_ + _)
    val pairs = flightsCount.map(tuple => (null, {
        val bson = new BasicBSONObject()
        bson.put("year", tuple._1._1)
        bson.put("month", tuple._1._2)
        bson.put("day", tuple._1._3)
        bson.put("origin", tuple._1._4)
        bson.put("destination", tuple._1._5)

        val javaint: java.lang.Integer = tuple._2 // Convert from scala.Int to java.lang.Integer
        bson.put("frequency", javaint)
        bson
      })
    )

    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/flights.routes")
    pairs.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[BSONObject],
      classOf[MongoOutputFormat[Object, BSONObject]],
      outputConfig
    )
  }
}