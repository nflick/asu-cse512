package asu.cse512.geospatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class GeoUnion {

	// the parent of each rectangle
	public static int[] unionSet;
	public static HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
	public final static Function<String, Rectangle> RECTANGLE_EXTRACTOR = new Function<String, Rectangle>() {
		private static final long serialVersionUID = 1L;

		public Rectangle call(String s) {
			// System.out.println("extractor 1");
			List<String> array = Arrays.asList(s.split(","));
			double x1 = 0.0, x2 = 0.0, y1 = 0.0, y2 = 0.0;
			int id = -1;
			x1 = Double.parseDouble(array.get(0));
			y1 = Double.parseDouble(array.get(1));
			x2 = Double.parseDouble(array.get(2));
			y2 = Double.parseDouble(array.get(3));
			Rectangle rectangle = new Rectangle(x1, y1, x2, y2);
			return rectangle;
		}
	};
	// Parses a line from the inut file into a JTS Geometry object.
	private static final Function<Rectangle, Geometry> RECTANGLE_TO_JTS = new Function<Rectangle, Geometry>() {
		public Geometry call(Rectangle r) throws ParseException {
			double x1 = r.getX1();
			double y1 = r.getY1();
			double x2 = r.getX2();
			double y2 = r.getY2();
			String s2 = String.format(
					"POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", x1, y1,
					x2, y1, x2, y2, x1, y2, x1, y1);
			Geometry g = new WKTReader().read(s2);
			return g;
		}
	};

	// Reduces an RDD by repeatedly unioning the geometries in it.
	private static final Function2<Geometry, Geometry, Geometry> REDUCER = new Function2<Geometry, Geometry, Geometry>() {
		public Geometry call(Geometry a, Geometry b) throws Exception {
			return a.union(b);
		}
	};
	private static final FlatMapFunction<Geometry, String> GEO_FORMATTER = new FlatMapFunction<Geometry, String>() {

		public Iterable<String> call(Geometry g) throws Exception {
			Coordinate[] cs = g.getCoordinates();
			ArrayList<String> array = new ArrayList<String>();
			for (int i = 0; i < cs.length - 1; i++) {
				Coordinate c = cs[i];
				array.add(c.x + "," + c.y);
			}
			return array;
		}

	};

	// Main geometric union method.
	public static void union(JavaSparkContext context, String input,
			String output) {
		long lStartTime = new Date().getTime();
		JavaRDD<String> file = context.textFile(input);
		// Map the input file to an RDD of Geometry objects.
		JavaRDD<Rectangle> rectangles = file.map(RECTANGLE_EXTRACTOR);
		List<Rectangle> list = rectangles.collect();
		int len = list.size();
		unionSet = new int[len];

		for (int i = 0; i < len; i++) {
			unionSet[i] = i;
		}
		for (int i = 0; i < len; i++) {
			Rectangle ri = list.get(i);
			for (int j = i + 1; j < len; j++) {
				if (ri.isOverlap(list.get(j))) {
					unionxy(i, j);
				}
			}
		}

		for (int i = 0; i < len; i++) {
			find(i);
			hashMap.put(list.get(i).getId(), unionSet[i]);
		}
		final Broadcast<HashMap<Integer, Integer>> broadcastHashMap = context
				.broadcast(hashMap);
		System.out.println("finish union find");
		long lEndTime = new Date().getTime();
		long difference = lEndTime - lStartTime;
		System.out.println("running time: " + difference);
		JavaRDD<Rectangle> rectangles2 = context.parallelize(list);
		JavaPairRDD<Integer, Rectangle> tuples = rectangles2
				.keyBy(new Function<Rectangle, Integer>() {

					public Integer call(Rectangle v1) throws Exception {
						// System.out.println("in keyby id= " + v1.getId());
						return broadcastHashMap.value().get(v1.getId());
					}

				});
		JavaPairRDD<Integer, Geometry> tuples2 = tuples.mapValues(
				RECTANGLE_TO_JTS).reduceByKey(REDUCER);
		JavaRDD<Geometry> polys = tuples2.values();
		JavaRDD<String> result = polys.flatMap(GEO_FORMATTER);

		// System.out.println("union result");
		// List<Geometry> list2 = polys.collect();
		// for (Geometry g : list2)
		// System.out.println(g);
		result.saveAsTextFile(output);

	}

	public static int find(int x) {
		int x2 = x;
		while (unionSet[x] != x) {
			x = unionSet[x];
		}
		while (unionSet[x2] != x2) {
			int next = unionSet[x2];
			unionSet[x2] = x;
			x2 = next;
		}
		return x;
	}

	public static void unionxy(int x, int y) {
		int px = find(x);
		int py = find(y);
		unionSet[px] = py;
	}

	public static void main(String[] args) {
		long lStartTime = new Date().getTime();
		// String inputPath = "/home/steve/Documents/q1/input1.txt";
		String inputPath = getFileN(10);
		String outputFolder = "/home/steve/Documents/q1/output1";
		JavaSparkContext context = getContext("Union");
		union(context, inputPath, outputFolder);
		long lEndTime = new Date().getTime();
		long difference = lEndTime - lStartTime;
		System.out.println("running time: " + difference);

	}

	// used for debug
	public static JavaSparkContext getContext(String appName) {
		SparkConf conf = new SparkConf().setAppName(appName).setMaster("local");
		conf.set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext context = new JavaSparkContext(conf);
		return context;
	}

	public static String getFileN(int N) {
		return String
				.format("/mnt/hgfs/uBuntu_share_folder/arealm/arealm_reduced_%d.csv",
						N);
	}
}