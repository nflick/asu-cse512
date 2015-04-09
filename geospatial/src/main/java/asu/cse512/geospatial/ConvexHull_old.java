package asu.cse512.geospatial;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class ConvexHull_old {
	// public static int MIN_X = -180;
	// public static int MAX_X = 180;
	// number of partition for the first convex hull
	public static int NUM_PARTITION = 700;

	// read the data from string, convert it to a point in JTS, also assign a
	// partition for each point
	// the key of the tuple is the partition number, the value of the tuple is
	// the point
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

	// union two geometry to one
	private static final Function2<Geometry, Geometry, Geometry> REDUCER = new Function2<Geometry, Geometry, Geometry>() {
		public Geometry call(Geometry a, Geometry b) throws Exception {
			// System.out.println("in reducer");
			// System.out.println(a);
			// System.out.println(b);
			// System.out.println(a.union(b));
			return a.union(b);
		}
	};

	// calculate convex hull and convert the result from polygon back to points
	private static final Function<Geometry, Geometry> FIRST_CONVEX_HULL = new Function<Geometry, Geometry>() {

		public Geometry call(Geometry g1) throws Exception {
			Geometry g2 = g1.convexHull();
			g2 = Common.convertToMultiPoints(g2);
			return g2;
		}
	};

	// public static int getPartitionIndex(double x) {
	// int n = NUM_PARTITION;
	// double idx = (x - MIN_X) / ((MAX_X - MIN_X) / n * 1.0);
	// int p = (int) idx;
	// if (p < 0)
	// p = 0;
	// if (p >= n)
	// p = n - 1;
	// return p;
	// }

	// partition the points randomly, better than range partition, especially
	// for skew data
	public static int getPartitionRandom(int x) {
		return x % NUM_PARTITION;
	}

	/**
	 * @param ctx
	 *            : java spark context
	 * @param input
	 *            : the input file path
	 * @param output
	 *            : the path of output folder
	 * @param writeToFile
	 *            : set to true to write the result to file
	 * @return
	 */
	public static Geometry convexHull(JavaSparkContext ctx, String input,
			String output, boolean writeToFile) {

		JavaRDD<String> file = ctx.textFile(input);

		JavaPairRDD<Integer, Geometry> tuples = file.mapToPair(POINT_EXTRACTOR);
		JavaPairRDD<Integer, Geometry> partitions = tuples.reduceByKey(REDUCER);
		JavaRDD<Geometry> values = partitions.values();
		JavaRDD<Geometry> first_convexhull = values.map(FIRST_CONVEX_HULL);
		Geometry g = first_convexhull.reduce(REDUCER);
		Geometry result = g.convexHull();
		System.out.println("convex hull result:");
		System.out.println(result);
		if (writeToFile) {
			Common.writeHDFSPoints(result, ctx, output);
		}
		// ctx.close();
		return result;
	}

	public static void main(String[] args) throws ParseException {
		String inputPath = getFileN(1000);
		// String inputPath =
		// "/mnt/hgfs/uBuntu_share_folder/ProjectTestCase/ConvexHullTestData.csv";
		// String inputPath = "/home/steve/Documents/q2/input1.txt";
		// String outputFolder = "/home/steve/Documents/q2/output1";
		String outputFolder = "/home/steve/Documents/q2/output1";
		JavaSparkContext context = getContext("ConvexHull");
		long lStartTime = new Date().getTime();
		convexHull(context, inputPath, outputFolder, true);
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
