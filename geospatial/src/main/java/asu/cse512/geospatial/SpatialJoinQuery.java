package asu.cse512.geospatial;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class SpatialJoinQuery implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args){
		String input1 = "/home/chencheng/Documents/input1.txt";
		String input2 = "/home/chencheng/Documents/input2.txt";
		String outputFolder = "/home/chencheng/Documents/output1.txt";
		SparkConf conf = new SparkConf().setAppName(
				"org.sparkexample.closest_pair").setMaster("local");
		conf.set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> file1 = context.textFile(input1);
		JavaRDD<String> file2 = context.textFile(input2);
		JavaRDD<Rectangle> rectA = file1.map(RECTANGLE_EXTRACTOR);
		JavaRDD<Rectangle> rectB = file2.map(RECTANGLE_EXTRACTOR);
		JavaPairRDD<Rectangle, Rectangle> cartestianTuples = rectA.cartesian(rectB);
		JavaPairRDD<Rectangle, Rectangle> tuples = cartestianTuples.filter(RANGE_FILTER);
		JavaPairRDD<String, String> idTuples = JavaPairRDD.fromJavaRDD(tuples.map(RECTANGLE_ID_CONVERTER));
		JavaPairRDD<String, String> result = idTuples.reduceByKey(AID_REDUCER);
		result.saveAsTextFile(outputFolder);
		context.close();
	}
	
	public final static Function<String, Rectangle> RECTANGLE_EXTRACTOR = 
			new Function<String, Rectangle>(){
		private static final long serialVersionUID = 1L;

		public Rectangle call(String s){
			List<String> array = Arrays.asList(s.split("\t"));
			double x1 = 0.0, x2 = 0.0, y1 = 0.0, y2 = 0.0;
			int id = 0;
			id = Integer.parseInt(array.get(0));
			x1 = Double.parseDouble(array.get(1));
			y1 = Double.parseDouble(array.get(2));
			x2 = Double.parseDouble(array.get(3));
			y2 = Double.parseDouble(array.get(4));
			Rectangle rectangle = new Rectangle(id, x1, y1, x2, y2);
			return rectangle;	
		}
	};
	
	public final static Function<Tuple2<Rectangle, Rectangle>, Tuple2<String, String>> RECTANGLE_ID_CONVERTER = 
			new Function<Tuple2<Rectangle, Rectangle>, Tuple2<String, String>>(){
		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(Tuple2<Rectangle, Rectangle> tuple){
			Rectangle rectA = tuple._1();
			Rectangle rectB = tuple._2();
			return new Tuple2<String, String>(rectA.getIdToString(),rectB.getIdToString());
		}
	};
	
	public final static Function<Tuple2<Rectangle, Rectangle>, Boolean> RANGE_FILTER = 
			new Function<Tuple2<Rectangle, Rectangle>, Boolean>(){
		private static final long serialVersionUID = 1L;
		public Boolean call(Tuple2<Rectangle, Rectangle> tuple){
			Rectangle rectA = tuple._1();
			Rectangle rectB = tuple._2();
			return rectB.isIn(rectA);
				
		}
	};
	
	public final static Function2<String, String, String> AID_REDUCER = 
			new Function2<String, String, String>(){
		
		private static final long serialVersionUID = 1L;

		public String call(String a,String b){
			return (a+", "+b);
		}
	};
	
	
	
	

}
