package asu.cse512.geospatial;

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
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class Q2_ConvexHull {
	public static int MIN_X = -160;
	public static int MAX_X = -120;

	public static Geometry convertToMultiPoints(Geometry g)
			throws ParseException {
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
				if (g2 == null)
					g2 = new WKTReader().read(str_point);
				else
					g2 = g2.union(new WKTReader().read(str_point));
			}
		} else {
			g2 = g;
		}
		return g2;
	}

	private static final PairFunction<String, Integer, Geometry> POINT_EXTRACTOR = new PairFunction<String, Integer, Geometry>() {
		public Tuple2<Integer, Geometry> call(String s) throws ParseException {
			List<String> array = Arrays.asList(s.split(","));
			double x = Double.parseDouble(array.get(0));
			double y = Double.parseDouble(array.get(1));
			String s2 = String.format("POINT  (%f %f)", x, y);
			Geometry g = new WKTReader().read(s2);
			int idx = getPartitionIndex(x);
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
			Geometry g2 = g1.convexHull();
			g2 = convertToMultiPoints(g2);
			// System.out.println("in first");
			// System.out.println(g1);
			// System.out.println(g2);
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

	public static Geometry convexHull(JavaSparkContext ctx, String input) {
		JavaRDD<String> file = ctx.textFile(input);

		JavaPairRDD<Integer, Geometry> tuples = file.mapToPair(POINT_EXTRACTOR);
		JavaPairRDD<Integer, Geometry> partitions = tuples.reduceByKey(REDUCER);
		JavaRDD<Geometry> values = partitions.values();
		JavaRDD<Geometry> first_convexhull = values.map(FIRST_CONVEX_HULL);
		// values.mapp
		Geometry g = first_convexhull.reduce(REDUCER);
		Geometry result = g.convexHull();
		System.out.println("convex hull result:");
		System.out.println(result);
		ctx.close();
		return result;
	}
}
