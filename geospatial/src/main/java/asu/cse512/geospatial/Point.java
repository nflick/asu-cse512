package asu.cse512.geospatial;

import java.io.Serializable;

public class Point implements Serializable {

	private static final long serialVersionUID = 1L;

	public Point(double x, double y) {
		this.x = x;
		this.y = y;
		this.id = hashCode();
	}

	private final double x, y;
	private final int id;

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}

	public double distance(Point point) {
		double dx = x - point.x;
		double dy = y - point.y;
		return dx * dx + dy * dy;
	}

	public int getId() {
		return id;
	}

	// @Override
	// public boolean equals(Object obj) {
	// if (obj == this) {
	// return true;
	// }
	//
	// if (!(obj instanceof Point)) {
	// return false;
	// }
	//
	// Point point = (Point) obj;
	// return this.id == point.getId();
	// }

	@Override
	public String toString() {
		return Double.toString(x) + "," + Double.toString(y);
	}

	public boolean isSame(Point p) {
		double epsilon = 0.000001;
		return Math.abs((this.x - p.x)) < epsilon
				&& Math.abs((this.y - p.y)) < epsilon;
	}

	public void print() {
		System.out.println("\npoint hash:" + this.id);
		System.out.println("x=" + this.x);
		System.out.println("y=" + this.y);
	}
}
