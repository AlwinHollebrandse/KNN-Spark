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
        val trainDataDF = spark.read.format("libsvm").option("vectorType", "dense").load(args(0)) // TODO cache train and/or test?
		val testDataDF = spark.read.format("libsvm").option("vectorType", "dense").load(args(2)) // TODO should be dense and not sparse

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


        println("testData: ", testDataDF)
        testDataDF.collect.foreach(println)
        testDataDF.show

        val nearestKClasses = findKNearestClasses(testDataDF, trainDataDF, k)
        // nearestKClasses.saveAsTextFile(args(1)) // TODO either write to file or remove arg
        println("nearestKClasses: ", nearestKClasses.mkString(" "))

        val testPointsActual = testDataDF.select("label").collect.map(row => row.getAs[Double](0))
        val accuracy = findAccuracy(testPointsActual, nearestKClasses)
        println("accuracy: ", accuracy)
        
        spark.stop()
    }

    def calcDistance(xs: Array[Double], ys: Array[Double]) = {
    // def calcDistance(xs: Row, ys: Row) = {
        sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
    }

    def findKNearestClasses(testDataDF: DataFrame, trainDataDF: DataFrame, k: Int): Array[Double] = {
        val trainData = trainDataDF.select("features").collect.map(row => row.getAs[DenseVector](0).toArray)
        // println("inner trainData: ")
        // trainData.foreach(println)
        val trainDataClasses = trainDataDF.select("label").collect.map(row => row.getAs[Double](0))
        // println("inner trainDataClasses: ")
        // trainDataClasses.foreach(println)
        // val foo: Nothing = trainDataClasses
        val testData = testDataDF.select("features").collect.map(row => row.getAs[DenseVector](0).toArray)
        // println("inner testData: ")
        // testData.map(row => println(row.toArray.mkString(" ")))
        // println("")

        testData.map { testInstance =>
            val distances = 
                (trainData zip trainDataClasses).map { case (trainInstance, c) =>
                    c -> calcDistance(testInstance, trainInstance)
                }
            val classes = distances.sortBy(_._2).take(k).map(_._1)
            // val foo: Nothing = classes
            val classCounts = classes.groupBy(identity).mapValues(_.size)
            println("classCounts: ", classCounts)
            classCounts.maxBy(_._2)._1
        }
    }

    def findAccuracy(testPointsActual: Array[Double], predictions: Array[Double]): Double = {
        // val foo: Nothing = testPointsActual zip predictions
        val numberCorrect = (testPointsActual zip predictions).filter { case (actual,prediction) =>
            println("actual: ", actual, ", prediction: ", prediction)
            actual == prediction
        }.size
        // val foo: Nothing = numberCorrect
        println("numberCorrect: ", numberCorrect)
        return numberCorrect.toDouble / testPointsActual.size.toDouble
    }


}