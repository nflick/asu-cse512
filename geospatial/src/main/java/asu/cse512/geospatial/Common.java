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
					.println("\twhere COMMAND is one of { closest-points, farthest-points, convex-hull }");
			return;
		}

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

		JavaSparkContext ctx = new JavaSparkContext(master, command, sparkHome,
				decodedPath);

		if (command.equals("closest-points")) {
			String output = args[4];
			JavaRDD<Point> points = readHDFSPointFile(ctx, input1);
			PointPair closest = ClosestPoints.closestPoints(ctx, points);
			writeHDFSPointPair(closest, ctx, output);
		} else if (command.equals("convex-hull")) {
			String output = args[4];
			Q2_ConvexHull.convexHull(ctx, input1, output, true);
		} else if (command.equals("range")) {
			String input2 = args[4];
			String output = args[5];
			SpatialRange.range(ctx, input1, input2, output);
		} else if (command.equals("join-query")) {
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

	public static JavaRDD<Point> readHDFSPointFile(JavaSparkContext ctx,
			String path) {
		JavaRDD<String> lines = ctx.textFile(path);
		return lines.map(PARSE_POINT_MAP);
	}

	public static void writeHDFSPointPair(PointPair pair, JavaSparkContext ctx,
			String path) {
		JavaRDD<Point> rdd = ctx.parallelize(
				Arrays.asList(pair.getA(), pair.getB())).coalesce(1);
		rdd.saveAsTextFile(path);
	}

	public static void writeHDFSPoints(Geometry g, JavaSparkContext ctx,
			String output) {
		// System.out.println(g);
		MultiPoint points = (MultiPoint) convertToMultiPoints(g);
		Coordinate[] cs = points.getCoordinates();
		ArrayList<String> al = new ArrayList<String>();
		for (Coordinate c : cs) {
			String[] strs = coordinateToStrings(c);
			String p = strs[0] + "," + strs[1];
			al.add(p);
			// System.out.println(p);
		}
		JavaRDD<String> rdd = ctx.parallelize(al).coalesce(1);
		rdd.saveAsTextFile(output);
	}

	public static String[] coordinateToStrings(Coordinate c) {
		String str = c.toString();
		String[] strs = str.substring(1, str.length() - 1).split(",");
		return strs;
	}

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
		} else if (type.startsWith("POLYGON")
				|| type.startsWith("MULTIPOLYGON")) {
			Coordinate[] array = g.getCoordinates();
			for (Coordinate c : array) {
				String[] strs = Common.coordinateToStrings(c);
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
}
