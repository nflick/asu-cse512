package asu.cse512.geospatial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class TwitterHeatMap implements Serializable {

	private static final long serialVersionUID = 1L;

	public static void heatMap(JavaSparkContext context, String input1,
			String input2, String output) {

		JavaRDD<String> file1 = context.textFile(input1);
		// Map the input file to an RDD of Rectangle objects;
		JavaRDD<Rectangle> rectA = file1.map(RECTANGLE_EXTRACTOR1);
		JavaRDD<Point> point = Common.readHDFSPointFile(context, input2);
		// Use broadcast variable to define the rectB;
		final Broadcast<List<Point>> bv = context.broadcast(point.collect());
		JavaRDD<Tuple2<Integer, Integer>> result = rectA
				.map(new Function<Rectangle, Tuple2<Integer, Integer>>() {

					public Tuple2<Integer, Integer> call(Rectangle v1)
							throws Exception {
						// TODO Auto-generated method stub
						int count = 0;
						Integer aid = v1.getId();
						List<Point> points = bv.getValue();
						for (int i = 0; i < points.size(); i++) {
							if (v1.isPointIn(points.get(i)))
								count++;
						}
						Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(
								aid, count);
						return tuple;
					}

				});
		// Change the format of the result;
		result.coalesce(1).saveAsTextFile(output);
		context.close();
	}

	// Extract the rectangles into list as String;
	// Store rectangle objects which has all the attributes needed;
	public final static Function<String, Rectangle> RECTANGLE_EXTRACTOR1= new Function<String, Rectangle>() {
		private static final long serialVersionUID = 1L;

		public Rectangle call(String s) {
			// System.out.println("extractor 1");
			List<String> array = Arrays.asList(s.split(","));
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

	// Old method used to test locally;
	public static void main(String[] args) {
		String base = "/mnt/hgfs/shared/";
		String input1 = base + "polygon.csv";
		String input2 = base + "point.csv";
		String outputFolder = "/home/steve/Documents/q6/output1";
		SparkConf conf = new SparkConf().setAppName(
				"org.sparkexample.closest_pair").setMaster("local");
		conf.set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext context = new JavaSparkContext(conf);
		TwitterHeatMap.heatMap(context, input1, input2, outputFolder);
	}

}
