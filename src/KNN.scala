package com.alwin; //

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
// import org.apache.spark.sql.functions
// import org.apache.spark.implicits._
import math._

object KNN {
    def main(args: Array[String]): Unit = { // TODO how to get arg values
        val spark = SparkSession
					.builder
					.appName("DecisionTree")
					.config("spark.master", "local[4]")
					.getOrCreate()
        // import spark.implicits._

        val k = args(3).toInt
		// Load the data stored in LIBSVM format as a DataFrame with dense vectors.
        val trainData = spark.read.format("libsvm").option("vectorType", "dense").load(args(0)) // TODO cache train and/or test?
		val testData = spark.read.format("libsvm").option("vectorType", "dense").load(args(2)) // TODO should be dense and not sparse

        // val testWithIndex = testData.zipWithIndex // ((a,0),(b,1),(c,2)) // TODO need .rdd.zip...?
        // val testIndexKey = testWithIndex.map{case (k,v) => (v,k)} // ((0,a),(1,b),(2,c))
		//Note that some RDDs, such as those returned by groupBy(), do not guarantee order of elements in a partition. The index assigned to each element is therefore not guaranteed, and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
        
        // val cart = trainData.cartesian(testIndexKey)

        // val knnMapped = cart.map({ cartRecord =>
        //     val trainPoint = cartRecord._1
        //     val testPoint = cartRecord._2
        //     val key = testPoint.getIndex() // TODO getIndex and getValue
        //     val distance = calcDistance(trainPoint, testPoint.getValue());
        //     val value = (distance, trainPoint.getLabel());
        //     (key, value)
		// });
		// knnMapped.saveAsTextFile(args(1)+"/knnMapped");


        println("testData: ", testData)
        testData.collect.foreach(println)
        testData.show

        val nearestKClasses = findKNearestClasses(testData, trainData, k)
        // nearestKClasses.saveAsTextFile(args(1))
        println("nearestKClasses: ", nearestKClasses.mkString(" "))

        spark.stop()
    }

    def calcDistance(xs: Array[Double], ys: Array[Double]) = {
    // def calcDistance(xs: Row, ys: Row) = {
        sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
    }

    // def findKNearestClasses(testPoints: Array[Array[Double]], trainPoints: Array[Array[Double]], k: Int): Array[Int] = {
    // def findKNearestClasses(testPoints: DataFrame, trainPoints: DataFrame, k: Int): Array[Int] = {
    def findKNearestClasses(testPointsDF: DataFrame, trainPointsDF: DataFrame, k: Int): Array[Double] = {
        val trainPoints = trainPointsDF.select("features").collect.map(row => row.getAs[DenseVector](0).toArray)
        println("inner trainPoints: ")
        trainPoints.foreach(println)
        val trainPointsClasses = trainPointsDF.select("label").collect.map(row => row.getAs[Double](0))
        println("inner trainPointsClasses: ")
        trainPointsClasses.foreach(println)
        // val foo: Nothing = trainPointsClasses
        // println("inner trainPoints string: ", trainPoints.foreach(e=>println(e.getClass)))
        val testPoints = testPointsDF.select("features").collect.map(row => row.getAs[DenseVector](0).toArray)
        println("inner testPoints: ")
        testPoints.map(row => println(row.toArray.mkString(" ")))
        println("")

        // val trainPoints = trainPointsDF.map(row=>row.getSeq[Double](1)).collect
        // println("trainPoints: ", trainPoints.foreach(println))
        // val testPoints = testPointsDF.map(row=>row.getAs[Array[DoubleType]](1).toArray).collect

        // val trainPoints = trainPointsDF.rdd.map(row=>row.getAs[Vector[Double]](1).toArray).collect
        // println("trainPoints: ", trainPoints)
        // val testPoints = testPointsDF.rdd.map(row=>row.getAs[Vector[Double]](1).toArray).collect

        // val trainPoints = trainPointsDF.rdd.map(row=>row.getAs[Array[Double]](1)).collect
        // println("trainPoints: ", trainPoints)
        // val testPoints = testPointsDF.rdd.map(row=>row.getAs[Array[Double]](1)).collect

        // val trainPoints = trainPointsDF.rdd.map(row=>row.getAs[Double](0) -> row.getAs[Vector](1).toArray).toDF("class","array")
        // println("trainPoints: ", trainPoints)
        // val testPoints = testPointsDF.rdd.map(row=>row.getAs[Double](0) -> row.getAs[Vector](1).toArray).toDF("class","array")

        // val trainPoints = trainPointsDF.rdd.map(row=>row.getDouble(1)).collect
        // println("trainPoints: ", trainPoints)
        // val testPoints = testPointsDF.rdd.map(row=>row.getDouble(1)).collect

        // val trainPoints = trainPointsDF.rdd.map(row=>row.getDouble(1).toArray).collect //trainPointsDF.rdd.map(row=>row.getDouble(0)).collect
        // val testPoints = testPointsDF.rdd.map(row=>row.getDouble(1).toArray).collect
        
        
        // return new Array[Int](1)

        // val trainPoints = trainPointsDF.select("features").collect.map(row => row.getAs[DenseVector](0).toArray)

        testPoints.map { testInstance =>
            val distances = 
                (trainPoints zip trainPointsClasses).map { case (trainInstance, c) =>
                    c->println("trainInstance: ", trainInstance.mkString(" "))
                    c -> calcDistance(testInstance, trainInstance)
                }
            println("distances: ")
            distances.foreach(println)
            val classes = distances.sortBy(_._2).take(k).map(_._1)
            println("classes: ")
            classes.foreach(println)
            // val foo: Nothing = classes
            val classCounts = classes.groupBy(identity).mapValues(_.size)
            println("classCounts: ", classCounts)
            classCounts.maxBy(_._2)._1
        }

        // testPoints.map { testInstance =>
        //     val distances = 
        //         trainPoints.zipWithIndex.map { case (trainInstance, c) =>
        //             c->println("trainInstance: ", trainInstance.mkString(" "))
        //             c -> calcDistance(testInstance, trainInstance)
        //         }
        //     // val foo: Nothing = distances
        //     println("distances: ")
        //     distances.foreach(println)
        //     val classes = distances.sortBy(_._2).take(k).map(_._1)
        //     println("classes: ")
        //     classes.foreach(println)
        //     // val foo: Nothing = classes
        //     val classCounts = classes.groupBy(identity).mapValues(_.size)
        //     println("classCounts: ", classCounts)
        //     classCounts.maxBy(_._2)._1
        // }
    }


}