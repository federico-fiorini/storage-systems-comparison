import java.io.File

import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.{SQLContext, Row, DataFrame}

import scala.RuntimeException

object PreprocessCSVforNeo4j {  
  def main(args: Array[String]) { 

  	val conf = new SparkConf().setAppName("Flights Dataset")
    val sc = new SparkContext(conf)

    val ext = List("csv")
    val fileList = getListOfFiles(new File("dataset"), ext)
 
 		var i = 0
 		for (csvFile <- fileList) {
	    val dataset = sc.textFile(csvFile.getPath()).cache()

	    val rows = dataset.map(dropCommas).map(r => r.replace("\"", "")).map(r => r.split(","))
	    val header = rows.first().map(s => s.toString.toLowerCase())
	    val withoutHeader: RDD[Array[String]] = dropHeader(rows)

	    var index = Map[String, Int]()
	    val columns = List("year", "month", "day_of_month", "origin", "origin_city_name",
	    	"origin_state_nm", "dest", "dest_city_name", "dest_state_nm")
	    
	    for (columnName <- columns) {
	      index = index + (columnName -> header.indexOf(columnName))
	    }

	    // Map and Reduce to calculate frequency
	    val mappedFlights = withoutHeader.map( row => (Tuple5(row(index("origin")), row(index("dest")), row(index("year")), row(index("month")), row(index("day_of_month"))) , 1))
	    val flightsData = mappedFlights.reduceByKey(_ + _)

	    // Get list of all airports
	    val airportIndexes = Array(
    		Tuple3(index("origin"), index("origin_city_name"), index("origin_state_nm")),
    		Tuple3(index("dest"), index("dest_city_name"), index("dest_state_nm"))
    	)

	    val airportData = withoutHeader.flatMap( row => {
	    	for (e <- airportIndexes) yield Tuple3(row(e._1), row(e._2), row(e._3))
	    })

	    // Generate csv files
	    generateAirportFile(
	    	"airports_" + i +".csv",
	    	airportData,
	    	tuple => Array(tuple._1, "Airport", tuple._2, tuple._3),
	    	"id:ID(Airport),:LABEL,city,state",
	    	distinct = true
	    )

	    generateRoutesFile(
	    	"routes_" + i +".csv",
	    	flightsData,
	    	tuple => Array(tuple._1._1, tuple._1._2, "FLIGHT_TO", tuple._1._3, tuple._1._4, tuple._1._5, tuple._2.toString),
	    	":START_ID(Airport),:END_ID(Airport),:TYPE,year,month,day,frequency",
	    	distinct = true
	    )

	    i = i + 1
		}
  }

  def dropCommas(line: String): String = {
    val fcomma = "\"[^,\"]+,[^,\"]+\"".r
    var newLine = line
    while (fcomma.findAllIn(newLine).length != 0) {
      newLine = newLine.replace(fcomma.findFirstIn(newLine).mkString, fcomma.findFirstIn(newLine).mkString.replaceAll(",", ""))
    }
    return newLine
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def dropHeader(data: RDD[Array[String]]): RDD[Array[String]] = {
		data.mapPartitionsWithIndex((idx, lines) => {
    	if (idx == 0) {
      	lines.drop(1)
    	}
    	lines
  	})
	}

	def merge(srcPath: String, dstPath: String, header: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    MyFileUtil.copyMergeWithHeader(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, header)
	}

	def generateRoutesFile(file: String, withoutHeader: RDD[Tuple2[Tuple5[String, String, String, String, String], scala.Int]], fn: Tuple2[Tuple5[String, String, String, String, String], scala.Int] => Array[String], header: String , distinct:Boolean = true, separator: String = ",") = {
		val outputFile = "tmp/" + file
		FileUtil.fullyDelete(new File(outputFile))

		val tmpFile = "tmp/" + System.currentTimeMillis() + "-" + file
		val rows: RDD[String] = withoutHeader.mapPartitions(lines => {
  		lines.map(line => {
    		fn(line).mkString(separator)
  		})
		})

		if (distinct) rows.distinct() saveAsTextFile tmpFile
		else rows.saveAsTextFile(tmpFile)

		merge(tmpFile, outputFile, header)

		FileUtil.fullyDelete(new File(tmpFile))
	}

	def generateAirportFile(file: String, withoutHeader: RDD[Tuple3[String, String, String]], fn: Tuple3[String, String, String] => Array[String], header: String , distinct:Boolean = true, separator: String = ",") = {
		val outputFile = "tmp/" + file
		FileUtil.fullyDelete(new File(outputFile))

		val tmpFile = "tmp/" + System.currentTimeMillis() + "-" + file
		val rows: RDD[String] = withoutHeader.mapPartitions(lines => {
  		lines.map(line => {
    		fn(line).mkString(separator)
  		})
		})

		if (distinct) rows.distinct() saveAsTextFile tmpFile
		else rows.saveAsTextFile(tmpFile)

		merge(tmpFile, outputFile, header)

		FileUtil.fullyDelete(new File(tmpFile))
	}
}
