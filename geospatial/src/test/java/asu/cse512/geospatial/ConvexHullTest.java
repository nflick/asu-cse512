package asu.cse512.geospatial;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConvexHullTest {

	public static JavaSparkContext ctx;

	@BeforeClass
	public static void init() {
		ctx = ConvexHull_old.getContext("convex-hull");
	}

	@Test
	public void test1() throws IOException {
		String base = "/mnt/hgfs/uBuntu_share_folder/ProjectTestCase/";
		String input = base + "ConvexHullTestData.csv";
		String expectedOutput = base + "ConvexHullResult.csv";
		testCommon(input, expectedOutput);

	}

	@Test
	public void test2() throws IOException {
		int N = 1000;
		String base = "/mnt/hgfs/uBuntu_share_folder/";
		String input = base + String.format("arealm/arealm_reduced_%d.csv", N);
		String expectedOutput = base
				+ String.format("junit/convex-hull-arealm_%d_result.txt", N);
		testCommon(input, expectedOutput);

	}

	public void testCommon(String input, String expectedOutput)
			throws IOException {

		String outputFolder = "/home/steve/Documents/q2/output1/";
		String outputFile = outputFolder + "part-00000";

		ConvexHull.convexHull(ctx, input, outputFolder, true);
		ArrayList<Point> myPoints = readPoints(outputFile);
		ArrayList<Point> expectedPoints = readPoints(expectedOutput);
		comparePoints(myPoints, expectedPoints);
	}

	public void comparePoints(ArrayList<Point> myPoints,
			ArrayList<Point> expectedPoints) {
		if (myPoints.size() != expectedPoints.size())
			fail("output points number:" + myPoints.size() + "\n"
					+ "expected points number:" + expectedPoints.size());
		for (Point p : myPoints) {
			boolean found = false;
			for (Point exp : expectedPoints) {
				if (p.isSame(exp)) {
					found = true;
					break;
				}
			}
			if (!found)
				fail("point :" + p + " is not found in expected result");
		}
	}

	public ArrayList<Point> readPoints(String input) throws IOException {
		ArrayList<Point> array = new ArrayList<Point>();
		BufferedReader br = new BufferedReader(new FileReader(input));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] parts = line.split(",");
			assert parts.length == 2;
			array.add(new Point(Double.parseDouble(parts[0]), Double
					.parseDouble(parts[1])));
		}
		br.close();
		return array;

	}

}