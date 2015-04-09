package asu.cse512.geospatial;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class SpatialRange {
	// extract the small rectangles into list as string
	// define an rectangle object which has all the attributes needed
	private static final Function<String, Rectangle> SMALL_RECT_EXTRACTOR = new Function<String, Rectangle>() {

		public Rectangle call(String v1) throws Exception {
			// TODO Auto-generated method stub
			List<String> array = Arrays.asList(v1.split(","));
			int id = 0;
			double x1 = 0, y1 = 0, x2 = 0, y2 = 0;
			if (array.size() < 5) {
				return null;
			}
			id = Integer.parseInt(array.get(0));
			x1 = Double.parseDouble(array.get(1));
			y1 = Double.parseDouble(array.get(2));
			x2 = Double.parseDouble(array.get(3));
			y2 = Double.parseDouble(array.get(4));
			Rectangle rec = new Rectangle(id, x1, y1, x2, y2);
			return rec;

		}
	};
	// extract the big rectangle into list, give the id -1
	private static final Function<String, Rectangle> WINDOW_EXTRACTOR = new Function<String, Rectangle>() {

		public Rectangle call(String v1) throws Exception {
			// TODO Auto-generated method stub
			List<String> array = Arrays.asList(v1.split(","));
			int id = -1;
			double x1 = 0, y1 = 0, x2 = 0, y2 = 0;
			if (array.size() < 4) {
				return null;
			}
			x1 = Double.parseDouble(array.get(0));
			y1 = Double.parseDouble(array.get(1));
			x2 = Double.parseDouble(array.get(2));
			y2 = Double.parseDouble(array.get(3));
			Rectangle rec = new Rectangle(id, x1, y1, x2, y2);
			return rec;
		}
	};

	public static void range(JavaSparkContext context, String input1,
			String input2, String output) {
		JavaRDD<String> file1 = context.textFile(input1);
		JavaRDD<String> file2 = context.textFile(input2);
		// Map the input file to an RDD of Rectangle objects.
		JavaRDD<Rectangle> small = file1.map(SMALL_RECT_EXTRACTOR);
		JavaRDD<Rectangle> big = file2.map(WINDOW_EXTRACTOR);
		// using broadcast variable to define the window,
		// make sure that every site will have the window variable
		final Broadcast<Rectangle> window = context.broadcast(big.first());
		// filter the small rectangles which is not in the windows
		JavaRDD<Rectangle> result = small
				.filter(new Function<Rectangle, Boolean>() {
					public Boolean call(Rectangle v1) throws Exception {
						return window.value().isIn(v1);
					}
				});

		result.coalesce(1).saveAsTextFile(output);
		context.close();
	}

	// the main function is for test
	//
	// public static void main(String[] args){
	// String input1="/mnt/hgfs/shared/RangeQueryTestData.csv";
	// String input2="/mnt/hgfs/shared/window.csv";
	// String output="/mnt/hgfs/shared/output";
	// SparkConf conf = new SparkConf().setAppName(
	// "org.sparkexample.Range").setMaster("local");
	// conf.set("spark.hadoop.validateOutputSpecs", "false");
	// JavaSparkContext context=new JavaSparkContext(conf);
	// range(context,input1,input2,output);
	// }
}
