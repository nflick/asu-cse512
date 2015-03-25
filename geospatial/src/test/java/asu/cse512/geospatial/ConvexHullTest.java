package asu.cse512.geospatial;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

public class ConvexHullTest {

	@Test
	public void test1() throws IOException {
		String base = "/mnt/hgfs/uBuntu_share_folder/ProjectTestCase/";
		String input = base + "ConvexHullTestData.csv";
		String outputFolder = "/home/steve/Documents/q2/output1/";
		String outputFile = outputFolder + "part-00000";
		String expectedOutput = base + "ConvexHullResult.csv";

		Q2_ConvexHull.convexHull(Q2_ConvexHull.getContext("convex-hull"),
				input, outputFolder, true);
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
