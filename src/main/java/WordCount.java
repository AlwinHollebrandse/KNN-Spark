package com.alwin;

import java.io.File;
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

	public static void main(String[] args) throws Exception
	{
		makeLIBSVMFiles();
		// SparkConf conf = new SparkConf().setAppName("Word Count Spark");
		// JavaSparkContext sc = new JavaSparkContext(conf);
		
		// JavaRDD<String> textFile = sc.textFile(args[0]);
		
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
	}

	public static void makeLIBSVMFiles() throws FileNotFoundException, IOException {
		String[] inputFileNames = {"superSmallTrain copy.txt", "smallTrain copy.txt", "mediumTrain copy.txt", "largeTrain copy.txt"};
		String[] outputFileNames = {"superSmallTrain.txt", "smallTrain.txt", "mediumTrain.txt", "largeTrain.txt"};
	
		for (int i = 0; i < inputFileNames.length; i++) {
			File inputFile = new File(inputFileNames[i]);
			Scanner myFileReader = new Scanner(inputFile);

			FileWriter outputFile = new FileWriter(outputFileNames[i]);
			// BufferedWriter bw = new BufferedWriter(outputFile);
		    // PrintWriter out = new PrintWriter(bw);
			while (myFileReader.hasNextLine()) {
				String currentLine = myFileReader.nextLine();
				System.out.println(currentLine);
				String[] values = currentLine.split(",");
				StringBuilder result = new StringBuilder("");
				result.append(values[values.length - 1]);
				for (int j = 0; j < values.length- 1; j++) {
					result.append(" " + (j+1) + ":" + values[j]);
				}
				result.append("\n");
				// out.println(result.toString());
				outputFile.write(result.toString());
			}
			myFileReader.close();
			outputFile.close();
		}
	}
}