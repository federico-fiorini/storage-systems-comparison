import java.io.File

import au.com.bytecode.opencsv.CSVParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.{SQLContext, Row, DataFrame}

import scala.RuntimeException

object PreprocessCSV {  
    def main(args: Array[String]) { 

    	val conf = new SparkConf().setAppName("Flights Dataset")
	    val sc = new SparkContext(conf)

        val d = new File("dataset")
        val fileList = d.listFiles.filter(_.isFile).toList
 
 		var i = 0
 		for (csvFile <- fileList) {
		    val flightData = sc.textFile(csvFile.getPath()).cache()
		    val withoutHeader: RDD[String] = dropHeader(flightData)
		    val splittedFlights = withoutHeader.map(row => row.split(','))

		    // Map and Reduce to calculate frequency
		    val mappedFlights = splittedFlights.map( row => (Tuple5(row(16), row(17), row(0), row(1), row(2)) , 1))
		    val reducedFlights = mappedFlights.reduceByKey(_ + _)

		    // Map and Reduce to get list of all airports
		    val mappedAirports = splittedFlights.flatMap( row => {
		    	val a = Array(16, 17)
		    	for (e <- a) yield (Tuple5(row(e), "", "", "", "") , 1)
		    })
		    val reducedAirports = mappedAirports.reduceByKey(_ + _)

		    //val backToArray = reducedFlights.map( tuple => Array(tuple._1._1, tuple._1._2, tuple._1._3, tuple._1._4, tuple._1._5, tuple._2.toString) )

		    generateFile(
		    	"airports_" + i +".csv",
		    	reducedAirports,
		    	tuple => Array(tuple._1._1, "Airport", tuple._1._1),
		    	// backToArray,
		    	// columns => Array(columns(0), "Airport", columns(0)),
		    	"id:ID(Airport),:LABEL,name",
		    	distinct = true
		    )

		    generateFile(
		    	"routes_" + i +".csv",
		    	reducedFlights,
		    	tuple => Array(tuple._1._1, tuple._1._2, "FLIGHT_TO", tuple._1._3, tuple._1._4, tuple._1._5, tuple._2.toString),
		    	// backToArray,
		    	// columns => Array(columns(0), columns(1), "FLIGHT_TO", columns(2), columns(3), columns(4), columns(5)),
		    	":START_ID(Airport),:END_ID(Airport),:TYPE,year,month,day,frequency",
		    	distinct = true
		    )

		    i = i + 1
		}
    }

    def dropHeader(data: RDD[String]): RDD[String] = {
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

	def generateFile(file: String, withoutHeader: RDD[Tuple2[Tuple5[String, String, String, String, String], scala.Int]], fn: Tuple2[Tuple5[String, String, String, String, String], scala.Int] => Array[String], header: String , distinct:Boolean = true, separator: String = ",") = {
  	//def generateFile(file: String, withoutHeader: RDD[Array[String]], fn: Array[String] => Array[String], header: String , distinct:Boolean = true, separator: String = ",") = {	
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
