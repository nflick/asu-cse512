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

public class SpatialJoinQuery implements Serializable {

	private static final long serialVersionUID = 1L;

	public static void joinQuery(JavaSparkContext context, String input1,
			String input2, String output) {
		
		JavaRDD<String> file1 = context.textFile(input1);
		JavaRDD<String> file2 = context.textFile(input2);
		JavaRDD<Rectangle> rectA = file1.map(RECTANGLE_EXTRACTOR);
		JavaRDD<Rectangle> rectB = file2.map(RECTANGLE_EXTRACTOR);
		final Broadcast<List<Rectangle>> bv = context.broadcast(rectB.collect());
		JavaRDD<Tuple2<Integer, ArrayList<Integer>>> result = rectA
				.map(new Function<Rectangle, Tuple2<Integer, ArrayList<Integer>>>() {

					public Tuple2<Integer, ArrayList<Integer>> call(Rectangle v1)
							throws Exception {
						// TODO Auto-generated method stub
						Integer aid = v1.getId();
						ArrayList<Rectangle> bigRects = (ArrayList<Rectangle>) bv
								.getValue();
						ArrayList<Integer> bids = new ArrayList<Integer>();
						for (int i = 0; i < bigRects.size(); i++) {
							if (bigRects.get(i).isIn(v1))
								bids.add(bigRects.get(i).getId());
						}
						Tuple2<Integer, ArrayList<Integer>> tuple = new Tuple2<Integer, ArrayList<Integer>>(
								aid, bids);
						return tuple;
					}

				});

		result.coalesce(1).saveAsTextFile(output);
		context.close();
	}

	public final static Function<String, Rectangle> RECTANGLE_EXTRACTOR = new Function<String, Rectangle>() {
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

	public final static Function<Tuple2<Rectangle, Rectangle>, Tuple2<String, String>> RECTANGLE_ID_CONVERTER = new Function<Tuple2<Rectangle, Rectangle>, Tuple2<String, String>>() {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, String> call(Tuple2<Rectangle, Rectangle> tuple) {
			Rectangle rectA = tuple._1();
			Rectangle rectB = tuple._2();
			return new Tuple2<String, String>(rectA.getIdToString(),
					rectB.getIdToString());
		}
	};

	public final static Function<Tuple2<Rectangle, Rectangle>, Boolean> RANGE_FILTER = new Function<Tuple2<Rectangle, Rectangle>, Boolean>() {
		private static final long serialVersionUID = 1L;

		public Boolean call(Tuple2<Rectangle, Rectangle> tuple) {
			Rectangle rectA = tuple._1();
			Rectangle rectB = tuple._2();
			return rectB.isIn(rectA);

		}
	};

	public final static Function2<String, String, String> AID_REDUCER = new Function2<String, String, String>() {

		private static final long serialVersionUID = 1L;

		public String call(String a, String b) {
			return (a + ", " + b);
		}
	};

	public static void main(String[] args) {
		String base = "/mnt/hgfs/uBuntu_share_folder/ProjectTestCase";
		String input1 = base + "/q6input1.txt";
		String input2 = base + "/q6input2.txt";
		String outputFolder = "/home/steve/Documents/q6/output1";
		SparkConf conf = new SparkConf().setAppName(
				"org.sparkexample.closest_pair").setMaster("local");
		conf.set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext context = new JavaSparkContext(conf);
		SpatialJoinQuery2.joinQuery(context, input1, input2, outputFolder);
	}

}
