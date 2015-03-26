package asu.cse512.geospatial;

import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import scala.Tuple2;

import java.io.Serializable;

public class FarthestPoints implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	// Maps a JTS Coordinate object to our own Point object.
	private static final Function<Coordinate, Point> COORDINATE_MAP =
			new Function<Coordinate, Point>() {
		private static final long serialVersionUID = 1L;
		
		public Point call(Coordinate coord) {
			return new Point(coord.x, coord.y);
		}
	};
	
	// Maps a tuple of two points to a PointPair object.
	private static final Function<Tuple2<Point, Point>, PointPair> PAIR_MAP = 
			new Function<Tuple2<Point, Point>, PointPair>() {
		private static final long serialVersionUID = 1L;
		
		public PointPair call(Tuple2<Point, Point> pair) {
			return new PointPair(pair._1(), pair._2());
		}
	};
	
	// Reduces point pairs to the pair with the highest distance between them.
	private static final Function2<PointPair, PointPair, PointPair> FARTHEST_REDUCER = 
			new Function2<PointPair, PointPair, PointPair>() {
		private static final long serialVersionUID = 1L;
		
		public PointPair call(PointPair a, PointPair b) {
			return a.distance() > b.distance() ? a : b;
		}
	};
	
	// Main farthest pair method.
	public static PointPair farthestPoints(JavaSparkContext context, String input) {
		// We know that the farthest pair must be contained in the set of points
		// that form the convex hull. Therefore, we can perform the convex hull operation,
		// which is O(nlogn) and then find the farthest pair in the resultant set,
		// rather than running the naive O(n^2) farthest pair algorithm.
		Geometry g = Q2_ConvexHull.convexHull(context, input, "", false);
		MultiPoint multipoint = (MultiPoint)Common.convertToMultiPoints(g);
		JavaRDD<Coordinate> coords = context.parallelize(Arrays.asList(multipoint.getCoordinates()));
		JavaRDD<Point> points = coords.map(COORDINATE_MAP);
		JavaRDD<PointPair> pairs = points.cartesian(points).map(PAIR_MAP);
		return pairs.reduce(FARTHEST_REDUCER);
	}
	
}
