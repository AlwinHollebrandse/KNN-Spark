package com.alwin;

import java.io.File; // TODO remove unneeded imports
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.util.Scanner;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class KNN {

	public static void main(String[] args) throws Exception {
		if (args.length != 4) { // TODO add check that instances of train and test are same length for each data point
			System.out.printf("Usage: KNN <input dir that holds train data> <output dir> <test data file> <k>\n");
			System.exit(-1);
		}
		  
		long startTime = System.nanoTime();

		Integer k = Integer.valueOf(args[3]); // k for kNN

		SparkConf conf = new SparkConf().setAppName("KNN Spark");
		SparkContext sc = new SparkContext(conf);

		// final Broadcast<Integer> broadcastK = sc.broadcast(k);
		
		// Load and parse the data files.
		JavaRDD<LabeledPoint> trainData = MLUtils.loadLibSVMFile(sc, args[0]).toJavaRDD();
		JavaRDD<LabeledPoint> testData = MLUtils.loadLibSVMFile(sc, args[2]).toJavaRDD(); // TODO what type are the values? and keys?

		trainData.cache();
		// int trainingFrequency = trainData.groupBy("label").count();
		// System.out.println("trainingFrequency: " + trainingFrequency);


		// System.out.println(trainData.rdd);
		System.out.println("HERE training data rdd: " + trainData.toString());

		// collect RDD for printing
        for(LabeledPoint line:trainData.collect()){
            System.out.println("* "+line.toString);
        } 

		// trainData.show();

		JavaPairRDD<LabeledPoint,LabeledPoint> cart = trainData.cartesian(testData);
		cart.saveAsTextFile(args[1]+"/cart");
		
		JavaPairRDD<String,Tuple2<Double,String>> knnMapped =
            //                              input                  K       V
            cart.mapToPair(new PairFunction<Tuple2<String,String>, String, Tuple2<Double,String>>() {
			@Override
			public Tuple2<String,Tuple2<Double,String>> call(Tuple2<String,String> cartRecord) {
				LabeledPoint trainPoint = cartRecord._1;
				LabeledPoint testPoint = cartRecord._2;
				
				double distance = Util.calcDistance(trainInstance, testInstance);
				String K = rRecordID; //  r.recordID
				Tuple2<Double,String> V = new Tuple2<Double,String>(distance, trainPoint.getLabel()); // TODO not string?
				return new Tuple2<String,Tuple2<Double,String>>(K, V);
			}
		});
		knnMapped.saveAsTextFile(args[1]+"/knnMapped");



		// JavaRDD<String> trainFile = sc.textFile(args[0]);
		// JavaRDD<String> testFile = sc.textFile(args[2]);
		
		// JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
		// 	public Iterator<String> call(String s) { return Arrays.stream(s.split(" ")).iterator(); }
		// });
		
		// JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
		// 	public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
		// });
		
		// JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
		// 	public Integer call(Integer a, Integer b) { return a + b; }
		// });
		
		// counts.saveAsTextFile(args[1]);

		long endTime = System.nanoTime();
		long totalTime = (endTime - startTime)/1000000;
		// computeAccuracy(args[1], totalTime);
	}
}