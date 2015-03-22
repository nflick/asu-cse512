package asu.cse512.geospatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class Q2_ConvexHull {
	public static int MIN_X = -180;
	public static int MAX_X = 180;

	public static Geometry convertToMultiPoints(Geometry g) {
		String type = g.toString();
		Geometry g2 = null;
		if (type.startsWith("LINESTRING")) {
			LineString ls = (LineString) g;
			for (int i = 0; i < ls.getNumPoints(); i++) {
				if (g2 == null)
					g2 = ls.getPointN(i);
				else
					g2 = g2.union(ls.getPointN(i));
			}
		} else if (type.startsWith("POLYGON")) {
			Polygon p = (Polygon) g;
			Coordinate[] array = p.getCoordinates();
			for (Coordinate c : array) {

				String str = c.toString();
				String[] strs = str.substring(1, str.length() - 1).split(",");
				String str_point = String.format("POINT  (%s %s)", strs[0],
						strs[1]);
				Geometry tmp = null;
				try {
					tmp = new WKTReader().read(str_point);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.err.println("parsing error");
				}
				if (g2 == null)
					g2 = tmp;
				else
					g2 = g2.union(tmp);
			}
		} else {
			g2 = g;
		}
		return g2;
	}

	private static final PairFunction<String, Integer, Geometry> POINT_EXTRACTOR = new PairFunction<String, Integer, Geometry>() {
		public Tuple2<Integer, Geometry> call(String s) throws ParseException {
			List<String> array = Arrays.asList(s.split(","));
			if (array.size() < 2)
				return null;
			double x = Double.parseDouble(array.get(0));
			double y = Double.parseDouble(array.get(1));
			String s2 = String.format("POINT  (%f %f)", x, y);
			Geometry g = new WKTReader().read(s2);
			int idx = getPartitionRandom(g.hashCode());
			return new Tuple2<Integer, Geometry>(idx, g);
		}
	};

	private static final Function2<Geometry, Geometry, Geometry> REDUCER = new Function2<Geometry, Geometry, Geometry>() {
		public Geometry call(Geometry a, Geometry b) throws Exception {
			// System.out.println("in reducer");
			// System.out.println(a);
			// System.out.println(b);
			// System.out.println(a.union(b));
			return a.union(b);
		}
	};

	private static final Function<Geometry, Geometry> FIRST_CONVEX_HULL = new Function<Geometry, Geometry>() {

		public Geometry call(Geometry g1) throws Exception {
			System.out.println("in first");
			Geometry g2 = g1.convexHull();
			System.out.println(g2);
			g2 = convertToMultiPoints(g2);
			System.out.println(g1);
			System.out.println(g2);
			return g2;
		}
	};

	public static int getPartitionIndex(double x) {
		int n = 3;
		double idx = (x - MIN_X) / ((MAX_X - MIN_X) / n * 1.0);
		int p = (int) idx;
		if (p < 0)
			p = 0;
		if (p >= n)
			p = n - 1;
		return p;
	}

	public static int getPartitionRandom(int x) {
		return x % 30;
	}

	public static Geometry convexHull(JavaSparkContext ctx, String input,
			String output, boolean writeToFile) {

		JavaRDD<String> file = ctx.textFile(input);

		JavaPairRDD<Integer, Geometry> tuples = file.mapToPair(POINT_EXTRACTOR);
		JavaPairRDD<Integer, Geometry> partitions = tuples.reduceByKey(REDUCER);
		JavaRDD<Geometry> values = partitions.values();
		JavaRDD<Geometry> first_convexhull = values.map(FIRST_CONVEX_HULL);
		// values.mapp
		Geometry g = first_convexhull.reduce(REDUCER);
		Geometry result = g.convexHull();
		if (writeToFile) {
			writeHDFSPoints(result, ctx, output);
		}
		// ctx.close();
		return result;
	}

	public static void writeHDFSPoints(Geometry g, JavaSparkContext ctx,
			String output) {
		System.out.println("convex hull result:");
		// System.out.println(g);
		MultiPoint points = (MultiPoint) convertToMultiPoints(g);
		Coordinate[] cs = points.getCoordinates();
		ArrayList<String> al = new ArrayList<String>();
		for (Coordinate c : cs) {
			String str = c.toString();
			str = str.substring(1, str.length() - 1);
			String[] strs = str.split(",");
			String p = strs[0] + "," + strs[1];
			al.add(p);
			System.out.println(p);
		}
		JavaRDD<String> rdd = ctx.parallelize(al).coalesce(1);
		rdd.saveAsTextFile(output);
	}

	public static void main(String[] args) throws ParseException {
		// String inputPath = getFileN(2);
		String inputPath = "/mnt/hgfs/uBuntu_share_folder/ProjectTestCase/ConvexHullTestData.csv";
		// String inputPath = "/home/steve/Documents/q2/input1.txt";
		// String outputFolder = "/home/steve/Documents/q2/output1";
		String outputFolder = "/home/steve/Documents/q2/output1";
		JavaSparkContext context = getContext("ConvexHull");
		convexHull(context, inputPath, outputFolder, true);

	}

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
