package asu.cse512.geospatial;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class Common {

	// This function is used to map an RDD of lines, as read from an HDFS
	// file to an RDD of Point objects.
	private static final Function<String, Point> PARSE_POINT_MAP = new Function<String, Point>() {
		private static final long serialVersionUID = 1L;

		public Point call(String s) throws Exception {
			String[] parts = s.split(",");
			assert parts.length == 2;
			return new Point(Double.parseDouble(parts[0]),
					Double.parseDouble(parts[1]));
		}
	};

	public static void main(String[] args) throws UnsupportedEncodingException {
		if (args.length < 5) {
			System.out
					.println("Usage: java -jar geospatial.jar <MASTER> <SPARK_HOME> <COMMAND> <ARG 1> [ARG 2] <OUTPUT>");
			System.out
					.println("\twhere COMMAND is one of { closest-points, farthest-points, convex-hull, range, join-query, union }");
			return;
		}

		// Reduce log level from INFO to WARN
		Logger log = Logger.getLogger("org");
		log.setLevel(Level.WARN);

		String master = args[0];
		String sparkHome = args[1];
		String command = args[2];
		String input1 = args[3];

		// Find the location of the currently running code.
		String path = Common.class.getProtectionDomain().getCodeSource()
				.getLocation().getPath();
		String decodedPath = URLDecoder.decode(path, "UTF-8");

		// Create Spark context. Note that this requires the location of the
		// currently running code, which we determined above.
		JavaSparkContext ctx = new JavaSparkContext(master, command, sparkHome,
				decodedPath);

		// Switch based on the command
		if (command.equals("closest-points")) {
			String output = args[4];
			JavaRDD<Point> points = readHDFSPointFile(ctx, input1);
			PointPair closest = ClosestPoints.closestPoints(ctx, points);
			writeHDFSPointPair(closest, ctx, output);
		} else if (command.equals("convex-hull")) {
			String output = args[4];
			ConvexHull.convexHull(ctx, input1, output, true);
		} else if (command.equals("range")) {
			// Range query has two input files, so the argument handling
			// is slightly different than the other cases.
			String input2 = args[4];
			String output = args[5];
			SpatialRange.range(ctx, input1, input2, output);
		} else if (command.equals("join-query")) {
			// Join query also has two input files, so argument handling is
			// again slightly different.
			String input2 = args[4];
			String output = args[5];
			SpatialJoinQuery.joinQuery(ctx, input1, input2, output);
		} else if (command.equals("union")) {
			String output = args[4];
			GeoUnion.union(ctx, input1, output);
		} else if (command.equals("farthest-points")) {
			String output = args[4];
			PointPair farthest = FarthestPoints.farthestPoints(ctx, input1);
			writeHDFSPointPair(farthest, ctx, output);
		} else {
			System.out.println("Unknown command.");
		}
	}

	// Takes the location of a file in HDFS which contains points,
	// and returns it in the form of an RDD.
	public static JavaRDD<Point> readHDFSPointFile(JavaSparkContext ctx,
			String path) {
		JavaRDD<String> lines = ctx.textFile(path);
		return lines.map(PARSE_POINT_MAP);
	}

	// Writes a point pair to HDFS.
	public static void writeHDFSPointPair(PointPair pair, JavaSparkContext ctx,
			String path) {
		JavaRDD<Point> rdd = ctx.parallelize(
				Arrays.asList(pair.getA(), pair.getB())).coalesce(1);
		rdd.saveAsTextFile(path);
	}

	// Writes a JTS Geometry object to HDFS.
	public static void writeHDFSPoints(Geometry g, JavaSparkContext ctx,
			String output) {
		MultiPoint points = (MultiPoint) convertToMultiPoints(g);
		Coordinate[] cs = points.getCoordinates();
		ArrayList<String> al = new ArrayList<String>();
		for (Coordinate c : cs) {
			// String[] strs = coordinateToStrings(c);
			String p = c.x + "," + c.y;
			al.add(p);
		}
		JavaRDD<String> rdd = ctx.parallelize(al).coalesce(1);
		rdd.saveAsTextFile(output);
	}

	// Returns an array of Strings containing the x and y coordinates,
	// respectively, of the given Coordinate.
	// public static String[] coordinateToStrings(Coordinate c) {
	// String str = c.toString();
	// String[] strs = str.substring(1, str.length() - 1).split(",");
	// return strs;
	// }

	// Utility method to convert a Geometry object to a JTS Multipoint object.
	// if you directly getCoordinates from Geometry, there might be duplicate
	// points, MultiPoints will remove duplicates
	public static Geometry convertToMultiPoints(Geometry g) {
		String type = g.getGeometryType().toUpperCase();
		Geometry g2 = null;
		if (type.equals("LINESTRING")) {
			LineString ls = (LineString) g;
			for (int i = 0; i < ls.getNumPoints(); i++) {
				if (g2 == null)
					g2 = ls.getPointN(i);
				else
					g2 = g2.union(ls.getPointN(i));
			}
		} else if (type.equals("POLYGON") || type.equals("MULTIPOLYGON")) {
			for (Coordinate c : g.getCoordinates()) {
				String str_point = String.format("POINT  (%f %f)", c.x, c.y);
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
}
