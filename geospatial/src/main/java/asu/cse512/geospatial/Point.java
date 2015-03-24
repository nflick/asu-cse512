package asu.cse512.geospatial;

import java.io.Serializable;

public class Point implements Serializable {

	private static final long serialVersionUID = 1L;

	public Point(double x, double y) {
		this.x = x;
		this.y = y;
	}

	private final double x, y;

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}

	public double distance(Point point) {
		return Math.sqrt(Math.pow(x - point.x, 2) + Math.pow(y - point.y, 2));
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (!(obj instanceof Point)) {
			return false;
		}

		Point point = (Point) obj;
		return point.x == x && point.y == y;
	}

	@Override
	public String toString() {
		return Double.toString(x) + "," + Double.toString(y);
	}

	public boolean isSame(Point p) {
		double epsilon = 0.000001;
		return Math.abs((this.x - p.x)) < epsilon
				&& Math.abs((this.y - p.y)) < epsilon;
	}
}
