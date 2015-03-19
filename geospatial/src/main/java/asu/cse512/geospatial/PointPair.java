package asu.cse512.geospatial;

import java.io.Serializable;

public class PointPair implements Serializable {

	private static final long serialVersionUID = 1L;

	public PointPair(Point a, Point b) {
		this.a = a;
		this.b = b;
		this.dist = a.distance(b);
	}
	
	private Point a, b;
	private double dist;
	
	public Point getA() {
		return a;
	}
	
	public Point getB() {
		return b;
	}
	
	public double distance() {
		return dist;
	}
	
	public boolean hasDistinctEndpoints() {
		return !a.equals(b);
	}
}
