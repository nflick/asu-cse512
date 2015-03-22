package asu.cse512.geospatial;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import java.io.Serializable;

public class FarthestPoints implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private static final Function<PointPair, Boolean> DIFFERENT_FILTER = 
			new Function<PointPair, Boolean>() {
		private static final long serialVersionUID = 1L;
		
		public Boolean call(PointPair segment) {
			return segment.hasDistinctEndpoints();
		}
	};
	
	private static final Function<Tuple2<Point, Point>, PointPair> SEGMENTS_MAP = 
			new Function<Tuple2<Point, Point>, PointPair>() {
		private static final long serialVersionUID = 1L;
		
		public PointPair call(Tuple2<Point, Point> pair) {
			return new PointPair(pair._1(), pair._2());
		}
	};
	
	private static final Function2<PointPair, PointPair, PointPair> FARTHEST_REDUCER = 
			new Function2<PointPair, PointPair, PointPair>() {
		private static final long serialVersionUID = 1L;
		
		public PointPair call(PointPair a, PointPair b) {
			return a.distance() > b.distance() ? a : b;
		}
	};
	
	public static PointPair farthestPoints(JavaRDD<Point> points) {
		JavaRDD<PointPair> segments = points.cartesian(points).map(SEGMENTS_MAP).filter(DIFFERENT_FILTER);
		return segments.reduce(FARTHEST_REDUCER);
	}
	
}
