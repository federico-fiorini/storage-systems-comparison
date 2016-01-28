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

import scala.collection.Map

object ImportToMongo {
  def main(args: Array[String]) { 

    val conf = new SparkConf().setAppName("Flights Dataset")
    val sc = new SparkContext(conf)

    cleanMongo()

    val ext = List("csv")
    val fileList = getListOfFiles(new File("dataset"), ext)
    for (csvFile <- fileList) {
      val dataset = sc.textFile(csvFile.getPath())
      saveToMongo(dataset)
    }
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
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

  def dropHeader(data: RDD[Array[String]]): RDD[Array[String]] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }

  def dropCommas(line: String): String = {
    val fcomma = "\"[^,\"]+,[^,\"]+\"".r
    var newLine = line
    while (fcomma.findAllIn(newLine).length != 0) {
      newLine = newLine.replace(fcomma.findFirstIn(newLine).mkString, fcomma.findFirstIn(newLine).mkString.replaceAll(",", ""))
    }
    return newLine
  }

  def saveToMongo(dataset: RDD[String]) {
    
    val rows = dataset.map(dropCommas).map(r => r.replace("\"", "")).map(r => r.split(","))
    val header = rows.first().map(s => s.toString.toLowerCase())
    val withoutHeader: RDD[Array[String]] = dropHeader(rows)

    var index = Map[String, Int]()
    val columns = List("year", "month", "day_of_month", "origin", "origin_city_name",
        "origin_state_nm", "dest", "dest_city_name", "dest_state_nm")
    
    for (columnName <- columns) {
      index = index + (columnName -> header.indexOf(columnName))
    }

    val tuples = withoutHeader.map(row => ( Tuple9(row(index("year")), row(index("month")), row(index("day_of_month")),
      row(index("origin")), row(index("origin_city_name")), row(index("origin_state_nm")), row(index("dest")),
      row(index("dest_city_name")), row(index("dest_state_nm"))), 1 ))

    val flightsCount = tuples.reduceByKey(_ + _)
    val pairs = flightsCount.map(tuple => (null, {
        
        val origin = new BasicBSONObject()
        origin.put("code", tuple._1._4)
        origin.put("city", tuple._1._5)
        origin.put("state", tuple._1._6)

        val dest = new BasicBSONObject()
        dest.put("code", tuple._1._7)
        dest.put("city", tuple._1._8)
        dest.put("state", tuple._1._9)

        val bson = new BasicBSONObject()
        bson.put("origin", origin)
        bson.put("destination", dest)
        bson.put("year", tuple._1._1)
        bson.put("month", tuple._1._2)
        bson.put("day", tuple._1._3)

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