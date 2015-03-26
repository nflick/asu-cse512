package asu.cse512.geospatial;

import java.io.Serializable;

public class Rectangle implements Serializable{

	private static final long serialVersionUID = 1L;
	private int id;
	private double x1;
	private double y1;
	private double x2;
	private double y2;

	public String getIdToString() {
		String idString = id + "";
		return idString;
	}

	public Rectangle(int id,double x1,double y1,double x2,double y2){
		this.id=id;
		this.x1=Math.min(x1, x2);
		this.y1=Math.max(y1, y2);
		this.x2=Math.max(x1, x2);
		this.y2=Math.min(y1, y2);
	}

	public boolean isIn(Rectangle small) {

		if(small==null){
			return false;
		}
		return x1 <= small.x1 && y1 >= small.y1 && x2 >= small.x2
				&& y2 <= small.y2;
	}

	@Override
	public String toString() {
			return Integer.toString(id);
	}
	
	public int getId() {
		return id;
	}
}
