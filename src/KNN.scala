package com.alwin;

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import math._

object KNN {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
					.builder
					.appName("DecisionTree")
					.config("spark.master", "local[4]")
					.getOrCreate()

        val k = args(2).toInt

        val startTime = System.nanoTime()

		// Load the data stored in LIBSVM format as a DataFrame with dense vectors.
        val trainDataDF = spark.read.format("libsvm").option("vectorType", "dense").load(args(0)) // TODO cache train and/or test?
		val testDataDF = spark.read.format("libsvm").option("vectorType", "dense").load(args(1))

        val nearestKClasses = findKNearestClasses(testDataDF, trainDataDF, k)

        val testPointsActual = testDataDF.select("label").collect.map(row => row.getAs[Double](0))
        val accuracy = findAccuracy(testPointsActual, nearestKClasses)
        println("The KNN classifier for " + testPointsActual.size + " instances required " + (System.nanoTime() - startTime)/1000000 + " ms CPU time, accuracy was " + accuracy);
        
        spark.stop()
    }

    def calcDistance(xs: Array[Double], ys: Array[Double]) = {
        sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
    }

    def findKNearestClasses(testDataDF: DataFrame, trainDataDF: DataFrame, k: Int): Array[Double] = {
        val trainData = trainDataDF.select("features").collect.map(row => row.getAs[DenseVector](0).toArray)
        val trainDataClasses = trainDataDF.select("label").collect.map(row => row.getAs[Double](0))
        val testData = testDataDF.select("features").collect.map(row => row.getAs[DenseVector](0).toArray)

        testData.map { testInstance =>
            val distances = 
                (trainData zip trainDataClasses).map { case (trainInstance, c) =>
                    c -> calcDistance(testInstance, trainInstance)
                }
            val classes = distances.sortBy(_._2).take(k).map(_._1)
            val classCounts = classes.groupBy(identity).mapValues(_.size)
            classCounts.maxBy(_._2)._1
        }
    }

    def findAccuracy(testPointsActual: Array[Double], predictions: Array[Double]): Double = {
        val numberCorrect = (testPointsActual zip predictions).filter { case (actual,prediction) =>
            actual == prediction
        }.size
        return numberCorrect.toDouble / testPointsActual.size.toDouble
    }
}