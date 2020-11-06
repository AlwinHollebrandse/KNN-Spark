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
		JavaRDD<LabeledPoint> trainData = MLUtils.loadLibSVMFile(sc, args[0]).toJavaRDD(); // TODO make tuplle with 1sst part being index? // zipWithIndex
		JavaPairRDD<Long, LabeledPoint> indexedTrainData = trainData
			.zipWithIndex()
			.map(new Function<Long, LabeledPoint, Long, LabeledPoint>() {
				// public Iterator<String> call(String s) { return Arrays.stream(s.split(" ")).iterator(); }
				// @Override
				public Tuple2<Long, LabeledPoint> call(LabeledPoint point, Long index) {
					return new Tuple2<Long, LabeledPoint>(index, point);
				}
		}); // TODO do this to test and/or train?
		//val indexKey = withIndex.map{case (k,v) => (v,k)}  //((0,a),(1,b),(2,c))
		//Note that some RDDs, such as those returned by groupBy(), do not guarantee order of elements in a partition. The index assigned to each element is therefore not guaranteed, and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
		JavaRDD<LabeledPoint> testData = MLUtils.loadLibSVMFile(sc, args[2]).toJavaRDD(); // loads sparse vector

		trainData.cache();
		// int trainingFrequency = trainData.groupBy("label").count();
		// System.out.println("trainingFrequency: " + trainingFrequency);

		// System.out.println(trainData.rdd);
		System.out.println("HERE training data rdd: " + trainData.toString());

		// collect RDD for printing
        for(LabeledPoint line:trainData.collect()){
            System.out.println("* "+line.toString());
		}


		// collect RDD for printing
        for(LabeledPoint line:indexedTrainData.collect()){
            System.out.println("* "+line.toString());
		}
// * (0.0,(7,[0,1,2,3,4,5,6],[0.0,0.0,0.0,0.0,0.0,0.0,0.0]))
// * (2.0,(7,[0,1,2,3,4,5,6],[2.0,2.0,2.0,2.0,2.0,2.0,2.0]))
// * (2.0,(7,[0,1,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0,1.0]))
// * (1.0,(7,[0,1,2,3,4,5,6],[10.0,10.0,10.0,10.0,10.0,10.0,10.0]))
// * (5.0,(7,[0,1,2,3,4,5,6],[10.0,10.0,10.0,10.0,10.0,10.0,10.0]))
// * (3.0,(7,[0,1,2,3,4,5,6],[12.0,12.0,12.0,12.0,12.0,12.0,12.0]))
// * (5.0,(7,[0,1,2,3,4,5,6],[11.0,11.0,11.0,11.0,11.0,11.0,11.0]))
// * (2.0,(7,[0,1,2,3,4,5,6],[2.0,2.0,2.0,2.0,2.0,2.0,2.0]))
// * (5.0,(7,[0,1,2,3,4,5,6],[110.0,110.0,11.0,11.0,11.0,11.0,11.0]))

		// trainData.show();

		JavaPairRDD<LabeledPoint,LabeledPoint> cart = trainData.cartesian(testData);
		cart.saveAsTextFile(args[1]+"/cart");
		
		JavaPairRDD<String,Tuple2<Double,String>> knnMapped =
            //                              input                  				  K       	V
            cart.mapToPair(new PairFunction<Tuple2<LabeledPoint,LabeledPoint>, String, Tuple2<Double,Double>>() {
			// @Override
			public Tuple2<String,Tuple2<Double,Double>> call(Tuple2<LabeledPoint,LabeledPoint> cartRecord) {
				LabeledPoint trainPoint = cartRecord._1;
				LabeledPoint testPoint = cartRecord._2;
				String[] rTokens = trainPoint.split(";"); // TODO need an index of test. that will be the key
				String rRecordID = rTokens[0];
		
				double distance = Util.calcDistance(trainPoint, testPoint);
				String K = rRecordID;//r.recordID; // TODO diff
				Tuple2<Double,Double> V = new Tuple2<Double,Double>(distance, trainPoint.getLabel()); // TODO not string?
				return new Tuple2<String,Tuple2<Double,Double>>(K, V);
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