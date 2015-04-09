package asu.cse512.geospatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;

public class ConvexHull {
	// public static int MIN_X = -180;
	// public static int MAX_X = 180;
	// number of partition for the first convex hull
	public static int NUM_PARTITION = 700;

	// read the data from string, convert it to a point in JTS, also assign a
	// partition for each point
	// the key of the tuple is the partition number, the value of the tuple is
	// the point
	private static final Function<String, Geometry> POINT_EXTRACTOR = new Function<String, Geometry>() {

		public Geometry call(String s) throws Exception {
			List<String> array = Arrays.asList(s.split(","));
			if (array.size() < 2)
				return null;
			double x = Double.parseDouble(array.get(0));
			double y = Double.parseDouble(array.get(1));
			String s2 = String.format("POINT  (%f %f)", x, y);
			Geometry g = new WKTReader().read(s2);
			return g;
		}

	};
	private static FlatMapFunction<Iterator<Geometry>, Geometry> PARTITION_MAP = new FlatMapFunction<Iterator<Geometry>, Geometry>() {
		private static final long serialVersionUID = 1L;

		public Iterable<Geometry> call(Iterator<Geometry> iter) {
			ArrayList<Geometry> geometries = new ArrayList<Geometry>();
			while (iter.hasNext()) {
				geometries.add(iter.next());
			}

			ArrayList<Geometry> result = new ArrayList<Geometry>();
			if (geometries.size() > 0) {
				Geometry g2 = CascadedPolygonUnion.union(geometries)
						.convexHull();
				g2 = Common.convertToMultiPoints(g2);
				result.add(g2);
			}

			return result;
		}
	};

	// union two geometry to one
	private static final Function2<Geometry, Geometry, Geometry> REDUCER = new Function2<Geometry, Geometry, Geometry>() {
		public Geometry call(Geometry a, Geometry b) throws Exception {
			return a.union(b);
		}
	};

	//
	// public static int getPartitionRandom(int x) {
	// return x % NUM_PARTITION;
	// }

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

		System.out.println("before point extractor");
		JavaRDD<Geometry> points = file.map(POINT_EXTRACTOR).repartition(
				NUM_PARTITION);

		System.out.println("before convexhull ");
		JavaRDD<Geometry> first_convexhull = points
				.mapPartitions(PARTITION_MAP);
		System.out.println("before reduce");
		Geometry g = first_convexhull.reduce(REDUCER);
		Geometry result = g.convexHull();
		System.out.println("convex hull result:");
		System.out.println(result);
		if (writeToFile) {
			Common.writeHDFSPoints(result, ctx, output);
		}
		return result;
	}

	public static void main(String[] args) throws ParseException {
		String inputPath = getFileN(1);
		// String inputPath =
		// "/mnt/hgfs/uBuntu_share_folder/ProjectTestCase/ConvexHullTestData.csv";
		// String inputPath = "/home/steve/Documents/q2/input1.txt";
		// String outputFolder = "/home/steve/Documents/q2/output1";
		String outputFolder = "/home/steve/Documents/q2/output1";
		JavaSparkContext context = getContext("ConvexHull");
		long lStartTime = new Date().getTime();
		ConvexHull.convexHull(context, inputPath, outputFolder, true);
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
