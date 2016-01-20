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

        var csvFile = "dataset/2008.csv"
 
	    if(csvFile == null || !new File(csvFile).exists()) {
	      throw new RuntimeException("Cannot find CSV file [" + csvFile + "]")
	    }
	 	 
	    val conf = new SparkConf().setAppName("Flights Dataset")
	    val sc = new SparkContext(conf)

	    val flightData = sc.textFile(csvFile).cache()
	    val withoutHeader: RDD[String] = dropHeader(flightData)

	    val splittedFlights = withoutHeader.map(row => row.split(','))
	    val mappedFlights = splittedFlights.map( row => ( Tuple5(row(16), row(17), row(0), row(1), row(2)) , 1))
	    val reducedFlights = mappedFlights.reduceByKey(_ + _)
	    //val backToArray = reducedFlights.map( tuple => Array(tuple._1._1, tuple._1._2, tuple._1._3, tuple._1._4, tuple._1._5, tuple._2.toString) )

	    // Generate Airports csv file. We assume that all the airports have at least one flight departing (no nodes with arrivals only)
	    generateFile(
	    	"airports.csv",
	    	reducedFlights,
	    	tuple => Array(tuple._1._1, "Airport", tuple._1._1),
	    	// backToArray,
	    	// columns => Array(columns(0), "Airport", columns(0)),
	    	"id:ID(Airport),:LABEL,name",
	    	distinct = true
	    )

	    generateFile(
	    	"routes.csv",
	    	reducedFlights,
	    	tuple => Array(tuple._1._1, tuple._1._2, "FLIGHT_TO", tuple._1._3, tuple._1._4, tuple._1._5, tuple._2.toString),
	    	// backToArray,
	    	// columns => Array(columns(0), columns(1), "FLIGHT_TO", columns(2), columns(3), columns(4), columns(5)),
	    	":START_ID(Airport),:END_ID(Airport),:TYPE,year,month,day,frequency",
	    	distinct = true
	    )
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
