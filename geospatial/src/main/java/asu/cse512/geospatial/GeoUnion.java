package asu.cse512.geospatial;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class GeoUnion {
	private static final Function<String, Geometry> POLYGON_EXTRACTOR = new Function<String, Geometry>() {
		public Geometry call(String s) throws ParseException {
			List<String> array = Arrays.asList(s.split(","));
			double x1 = Double.parseDouble(array.get(0));
			double y1 = Double.parseDouble(array.get(1));
			double x2 = Double.parseDouble(array.get(2));
			double y2 = Double.parseDouble(array.get(3));
			String s2 = String.format(
					"POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", x1, y1,
					x2, y1, x2, y2, x1, y2, x1, y1);
			Geometry g = new WKTReader().read(s2);
			return g;
		}
	};

	private static final Function2<Geometry, Geometry, Geometry> REDUCER = new Function2<Geometry, Geometry, Geometry>() {
		public Geometry call(Geometry a, Geometry b) throws Exception {
			return a.union(b);
		}
	};

	public static void union(JavaSparkContext context, String input,
			String output) {
		JavaRDD<String> file = context.textFile(input);
		JavaRDD<Geometry> rectangles = file.map(POLYGON_EXTRACTOR);
		Geometry result = rectangles.reduce(REDUCER);
		System.out.println("union result");
		System.out.println(result);
		Common.writeHDFSPoints(result, context, output);
		context.close();
	}

	public static void main(String[] args) {
		String base = "/home/steve/Documents/q1";
		String input1 = base + "/input1.txt";
		String outputFolder = base + "/output1";
		union(Q2_ConvexHull.getContext("union"), input1, outputFolder);
	}

}