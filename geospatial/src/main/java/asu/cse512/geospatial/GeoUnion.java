package asu.cse512.geospatial;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.union.*;

public class GeoUnion {

	// Parses a line from the inut file into a JTS Geometry object.
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

	// Reduces an RDD by repeatedly unioning the geometries in it.
	private static final Function2<Geometry, Geometry, Geometry> REDUCER = new Function2<Geometry, Geometry, Geometry>() {
		public Geometry call(Geometry a, Geometry b) throws Exception {
			return a.union(b);
		}
	};

	// Allows us to perform a more efficient union by unioning an entire
	// partition
	// together at once. This uses the JTS CascadedPolygonUnion operation to do
	// an efficient union on all of the Geometries in a single partition at the
	// same time.
	private static FlatMapFunction<Iterator<Geometry>, Geometry> PARTITION_MAP = new FlatMapFunction<Iterator<Geometry>, Geometry>() {
		private static final long serialVersionUID = 1L;

		public Iterable<Geometry> call(Iterator<Geometry> iter) {
			ArrayList<Geometry> geometries = new ArrayList<Geometry>();
			while (iter.hasNext()) {
				geometries.add(iter.next());
			}

			ArrayList<Geometry> result = new ArrayList<Geometry>();
			result.add(CascadedPolygonUnion.union(geometries));
			return result;
		}
	};

	// Main geometric union method.
	public static void union(JavaSparkContext context, String input,
			String output) {
		JavaRDD<String> file = context.textFile(input);
		// Map the input file to an RDD of Geometry objects.
		JavaRDD<Geometry> rectangles = file.map(POLYGON_EXTRACTOR);
		// Union partitions. The entire set of Geometry objects composing each
		// partition
		// will be unioned into a single Geometry. The end result will be a new
		// RDD with
		// a single Geometry object for each partition of Geometries that
		// existed in the
		// previous RDD>
		JavaRDD<Geometry> partitionUnions = rectangles
				.mapPartitions(PARTITION_MAP);
		// Reduce these remaining geometries with JTS's union operation.
		Geometry result = partitionUnions.reduce(REDUCER);
		System.out.println(result);
		Common.writeHDFSPoints(result, context, output);
		context.close();
	}



}