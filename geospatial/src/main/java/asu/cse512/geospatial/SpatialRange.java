package asu.cse512.geospatial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;



public class SpatialRange {
private static Rectangle window;
private static final Function<String,Rectangle> SMALL_RECT_EXTRACTOR =
		new Function<String,Rectangle>(){

			public Rectangle call(String v1) throws Exception {
				// TODO Auto-generated method stub
				List<String> array =  Arrays.asList(v1.split(","));
				int id=0;
				double x1=0,y1=0,x2=0,y2=0;
				if(array.size()<5){
					return null;
				}
				id=Integer.parseInt(array.get(0));
				x1=Double.parseDouble(array.get(1));
				y1=Double.parseDouble(array.get(2));
				x2=Double.parseDouble(array.get(3));
				y2=Double.parseDouble(array.get(4));
				Rectangle rec=new Rectangle(id,x1,y1,x2,y2);
				return rec;

			}	
 };
 private static final Function<String,Rectangle> WINDOW_EXTRACTOR =
			new Function<String,Rectangle>(){

	public Rectangle call(String v1) throws Exception {
		// TODO Auto-generated method stub
		List<String> array =  Arrays.asList(v1.split(","));
		int id=-1;
		double x1=0,y1=0,x2=0,y2=0;
		if(array.size()<4){
			return null;
		}
		x1=Double.parseDouble(array.get(0));
		y1=Double.parseDouble(array.get(1));
		x2=Double.parseDouble(array.get(2));
		y2=Double.parseDouble(array.get(3));
		Rectangle rec=new Rectangle(id,x1,y1,x2,y2);
		return rec;
	}		
 };
 private static final Function<Rectangle, Boolean> FILTER 
 		= new Function<Rectangle, Boolean>(){

			public Boolean call(Rectangle v1) throws Exception {
				// TODO Auto-generated method stub
				
				return window.isIn(v1);
			}
	 
 };
 
 public static void range(JavaSparkContext context,String input1,String input2,String output){
	 
	 
	JavaRDD<String> file1=context.textFile(input1);
	JavaRDD<String> file2=context.textFile(input2);
	JavaRDD<Rectangle> small=file1.map(SMALL_RECT_EXTRACTOR);
	JavaRDD<Rectangle> big=file2.map( WINDOW_EXTRACTOR);
	window=big.first();
	JavaRDD<Rectangle> result=small.filter(FILTER);
	
	result.saveAsTextFile(output);
	context.close();

	 
 }



public static void main(String[] args){
	 String input1="/mnt/hgfs/shared/RangeQueryTestData.csv";
	 String input2="/mnt/hgfs/shared/window.csv";
	 String output="/mnt/hgfs/shared/output";
	 SparkConf conf = new SparkConf().setAppName(
				"org.sparkexample.Range").setMaster("local");
		conf.set("spark.hadoop.validateOutputSpecs", "false");
	JavaSparkContext context=new JavaSparkContext(conf);
	 range(context,input1,input2,output);
 }
}
