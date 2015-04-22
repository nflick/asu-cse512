package asu.cse512.geospatial;

import java.io.Serializable;

public class Rectangle implements Serializable {

	private static final long serialVersionUID = 1L;
	private int id;
	private double x1;
	private double y1;
	private double x2;

	public double getX1() {
		return x1;
	}

	public double getY1() {
		return y1;
	}

	public double getX2() {
		return x2;
	}

	public double getY2() {
		return y2;
	}

	private double y2;

	public String getIdToString() {
		String idString = id + "";
		return idString;
	}

	public Rectangle(int id, double x1, double y1, double x2, double y2) {
		this.id = id;
		this.x1 = Math.min(x1, x2);
		this.y1 = Math.max(y1, y2);
		this.x2 = Math.max(x1, x2);
		this.y2 = Math.min(y1, y2);
	}

	public Rectangle(double x1, double y1, double x2, double y2) {
		this.id = hashCode();
		// System.out.println("in rec constructor id= "+id);
		this.x1 = Math.min(x1, x2);
		this.y1 = Math.max(y1, y2);
		this.x2 = Math.max(x1, x2);
		this.y2 = Math.min(y1, y2);
	}

	public boolean isIn(Rectangle small) {

		if (small == null) {
			return false;
		}
		return x1 <= small.x1 && y1 >= small.y1 && x2 >= small.x2
				&& y2 <= small.y2;
	}

	public boolean isPointIn(Point po) {

		if (po == null) {
			return false;
		}
		return x1 <= po.getX() && y1 >= po.getY() && x2 >= po.getX()
				&& y2 <= po.getY();
	}

	@Override
	public String toString() {
		return Integer.toString(id);
	}

	public int getId() {
		return id;
	}

	public boolean isOverlap(Rectangle rec) {
		boolean f1 = between(rec.x1, x1, x2) || between(rec.x2, x1, x2);
		boolean f2 = f1 || between(x1, rec.x1, rec.x2)
				|| between(x2, rec.x1, rec.x2);
		boolean f3 = between(rec.y1, y1, y2) || between(rec.y2, y1, y2);
		boolean f4 = f3 || between(y1, rec.y1, rec.y2)
				|| between(y2, rec.y1, rec.y2);
		return f2 && f4;
	}

	public static boolean between(double x, double a, double b) {
		if (a <= b)
			return a <= x && x <= b;
		return b <= x && x <= a;
	}
}
