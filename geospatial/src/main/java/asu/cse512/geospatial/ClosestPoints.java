package asu.cse512.geospatial;

import java.util.List;
import org.apache.spark.api.java.*;

import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.io.Serializable;

public class ClosestPoints implements Serializable {
	
	private static final long serialVersionUID = 1L;

	// This filter ensures that the distance between two identical points
	// is not considered, or else the result would always be 0.
	private static final Function<PointPair, Boolean> DIFFERENT_FILTER = 
			new Function<PointPair, Boolean>() {
		private static final long serialVersionUID = 1L;
		
		public Boolean call(PointPair segment) {
			return segment.hasDistinctEndpoints();
		}
	};
	
	// Reduces pairs of points to the pair that has the lowest distance.
	private static final Function2<PointPair, PointPair, PointPair> CLOSEST_REDUCER = 
			new Function2<PointPair, PointPair, PointPair>() {
		private static final long serialVersionUID = 1L;
		
		public PointPair call(PointPair a, PointPair b) {
			return a.distance() < b.distance() ? a : b;
		}
	};
	
	// THe primary function that performs the closest pair operation.
	public static PointPair closestPoints(JavaSparkContext context, JavaRDD<Point> points) {
		// Load points into a broadcast variable
		final Broadcast<List<Point>> broadcastPoints = context.broadcast(points.collect());
		
		// This class maps each point to a PointPair containing itself, and the closest other point to it.
		// This function must be defined here, rather than as a static member of this class,
		// so that it can capture the broadcast variable.
		JavaRDD<PointPair> pairs = points.map(new Function<Point, PointPair>() {
			private static final long serialVersionUID = 1L;

			public PointPair call(Point point) {
				PointPair closest = null;
				for (Point p : broadcastPoints.value()) {
					PointPair pair = new PointPair(point, p);
					if (!point.equals(p) && (closest == null || pair.distance() < closest.distance())) {
						closest = pair;
					}
				}
				
				return closest;
			}
		});
		
		return pairs.reduce(CLOSEST_REDUCER);
	}
	
}
