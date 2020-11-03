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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) throws Exception {
		if (args.length != 4) { // TODO add check that instances of train and test are same length for each data point
			System.out.printf("Usage: KNN <input dir that holds train data> <output dir> <test data file> <k>\n");
			System.exit(-1);
		}
		  
		long startTime = System.nanoTime();

		Integer k = Integer.valueOf(args[3]); // k for kNN

		SparkConf conf = new SparkConf().setAppName("KNN Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);

		final Broadcast<Integer> broadcastK = sc.broadcast(k);
		
		// Load and parse the data files.
		JavaRDD<LabeledPoint> trainData = MLUtils.loadLibSVMFile(sc, args[0]).toJavaRDD();
		JavaRDD<LabeledPoint> testData = MLUtils.loadLibSVMFile(sc, args[2]).toJavaRDD();


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
		
		counts.saveAsTextFile(args[1]);

		long endTime = System.nanoTime();
			long totalTime = (endTime - startTime)/1000000;
			computeAccuracy(args[1], totalTime);
	}
}