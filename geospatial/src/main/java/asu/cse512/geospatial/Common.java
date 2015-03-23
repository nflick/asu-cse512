package asu.cse512.geospatial;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Geometry;
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

		JavaSparkContext ctx = new JavaSparkContext(master, command, sparkHome, decodedPath);

		if (command.equals("closest-points")) {
			String output = args[4];
			JavaRDD<Point> points = readHDFSPointFile(ctx, input1);
			PointPair closest = ClosestPoints.closestPoints(points);
			writeHDFSPointPair(closest, ctx, output);
		} else if (command.equals("convex-hull")) {
			String output = args[4];
			Q2_ConvexHull.convexHull(ctx, input1, output, true);
		} else if(command.equals("range")){
			String input2=args[4];
			String output=args[5];
			SpatialRange.range(ctx,input1,input2,output);
		} else if(command.equals("join-query")){
			String input2=args[4];
			String output=args[5];
			SpatialJoinQuery.joinQuery(ctx,input1,input2,output);
			
		} else if(command.equals("union")){
			String input=args[4];
			String output=args[5];
			GeoUnion.union(ctx,input,output);
		}
			
		}else if (command.equals("farthest-points")) {
			String output = args[4];
			JavaRDD<Point> points = readHDFSPointFile(ctx, input1);
			PointPair farthest = FarthestPoints.farthestPoints(points);
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
		JavaRDD<Point> rdd = ctx.parallelize(Arrays.asList(pair.getA(),
				pair.getB())).coalesce(1);
		rdd.saveAsTextFile(path);
	}
}
