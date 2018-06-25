package com.test.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.api.java.function.Function2;

import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class JavaSparkConn {
	public static final String master = "spark://47.93.200.199:7077";
	// public static final String master = "spark://master:9000";

	public static void main(String[] args) {
		exec();
	}

	public static void exec() {
		try {
			SparkConf conf = new SparkConf().setAppName("Test").setMaster(master);
			JavaSparkContext jsc = new JavaSparkContext(conf);
			JavaRDD<String> lines = jsc.textFile("C:/Users/JUSFOUN/Desktop/log.txt");
			JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<String> call(String line) throws Exception {
					// TODO Auto-generated method stub
					return (Iterator<String>) Arrays.asList(line.split(" "));
				}
			});

			JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override

				public Tuple2<String, Integer> call(String word) throws Exception {

					// TODO Auto-generated method stub

					return new Tuple2<String, Integer>(word, 1);

				}

			});

			JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() { // 对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override

				public Integer call(Integer v1, Integer v2) throws Exception {

					// TODO Auto-generated method stub

					return v1 + v2;

				}

			});

			wordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override

				public void call(Tuple2<String, Integer> pairs) throws Exception {

					// TODO Auto-generated method stub

					System.err.println("-----------------输出--------------------" + pairs._1 + " : " + pairs._2);

				}

			});

			jsc.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
