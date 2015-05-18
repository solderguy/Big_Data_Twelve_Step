package jsoper.pair.s09hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class S09_junit_tests {
	MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
	ReduceDriver<Text, Text, Text, Text> reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
	static PointCleaner.ZoneCleanReducer tester = null;

	// Check that mapper calculates correct zone value for a point
	// and emits it as the key

	 @Test
	 public void test_map1() {
	 mapDriver.withMapper(new PointCleaner.ZoneCleanMapper())
	 .withInput(new LongWritable(10), new Text("71,19,4"))
	 .withOutput(new Text("2_0"), new Text("71_19")).runTest();
	 // System.out.println("expected output:" +
	 // mapDriver.getExpectedOutputs());
	 }
	
	 @Test
	 public void test_map2() {
	 mapDriver.withMapper(new PointCleaner.ZoneCleanMapper())
	 .withInput(new LongWritable(10), new Text("5,3,2"))
	 .withOutput(new Text("0_0"), new Text("5_3")).runTest();
	 }
	
	 @Test
	 public void test_map3() {
	 mapDriver.withMapper(new PointCleaner.ZoneCleanMapper())
	 .withInput(new LongWritable(10), new Text("250,191,2"))
	 .withOutput(new Text("8_6"), new Text("250_191")).runTest();
	 }
	
	 // Check that reducer keeps points adjecent to each other and drops those
	 // that are not
	
	 @Test
	 public void reducerTest1() {
	 List<Text> valueList = new ArrayList<Text>();
	 valueList.addAll(Arrays.asList(new Text("96_32"), new Text("96_33"),
	 new Text("96_34")));
	 reduceDriver.withReducer(new PointCleaner.ZoneCleanReducer())
	 .withInput(new Text("3_1"), valueList)
	 .withOutput(new Text(""), new Text("96,32\n96,33\n96,34\n"))
	 .runTest();
	 }
	
	 @Test
	 public void reducerTest2() {
	 List<Text> valueList = new ArrayList<Text>();
	 valueList.addAll(Arrays.asList(new Text("96_32"), new Text("96_33"),
	 new Text("96_35")));
	 reduceDriver.withReducer(new PointCleaner.ZoneCleanReducer())
	 .withInput(new Text("3_1"), valueList)
	 .withOutput(new Text(""), new Text("96,32\n96,33\n")).runTest();
	 }
	
	 @Test
	 public void reducerTest3() {
	 List<Text> valueList = new ArrayList<Text>();
	 valueList.addAll(Arrays.asList(new Text("5_5"), new Text("5_6"),
	 new Text("5_7"), new Text("4_3"), new Text("5_10")));
	 reduceDriver.withReducer(new PointCleaner.ZoneCleanReducer())
	 .withInput(new Text("0_0"), valueList)
	 .withOutput(new Text(""), new Text("5,5\n5,6\n5,7\n"))
	 .runTest();
	 }
	
	 // Test non-MR methods with reflection because they are private
	

	@Test
	public void testcalculateZone() throws Exception {
		Class<?> c = Class
				.forName("jsoper.pair.s09hadoop.PointCleaner$ZoneCleanMapper");
		Object i = c.newInstance();
		Method m = c.getDeclaredMethod("calculateZone",
				new Class[] { String[].class });
		m.setAccessible(true);
		PointCleaner.arrayShift = 0;

		String[] tokens = { "59", "118" };
		String zone = (String) m.invoke(i, new Object[] { tokens });
		assertEquals(zone, "1_3");

		PointCleaner.arrayShift = 15;
		zone = (String) m.invoke(i, new Object[] { tokens });
		assertEquals(zone, "2_4");
	}

	@Test
	public void testClearArray() throws Exception {
		Class<?> c = Class
				.forName("jsoper.pair.s09hadoop.PointCleaner$ZoneCleanReducer");
		Object i = c.newInstance();
		int[][] grid = { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } };
		// System.out.println("grid: " + Arrays.deepToString(grid));

		Method m = c.getDeclaredMethod("clearArray",
				new Class[] { int[][].class });
		m.setAccessible(true);
		m.invoke(i, new Object[] { grid });
		String contents = Arrays.deepToString(grid);
		assertTrue(contents.contains("[[0, 0, 0], [0, 0, 0], [0, 0, 0]]"));
	}

	@Test
	public void testHasNeighbor() throws Exception {
		Class<?> c = Class
				.forName("jsoper.pair.s09hadoop.PointCleaner$ZoneCleanReducer");
		Object i = c.newInstance();
		int[][] grid = { { 1, 0, 1, 0, 0 }, { 0, 0, 1, 1, 0 },
				{ 1, 0, 0, 0, 0 }, { 1, 0, 0, 0, 0 }, { 1, 0, 0, 0, 0 } };
		// System.out.println("grid: " + Arrays.deepToString(grid));

		Method m = c.getDeclaredMethod("hasNeighbor", new Class[] { int.class,
				int.class, int[][].class });
		m.setAccessible(true);

		assertTrue((boolean) m.invoke(i, new Object[] { 1, 1, grid }));
		assertTrue((boolean) m.invoke(i, new Object[] { 1, 2, grid }));
		assertTrue((boolean) m.invoke(i, new Object[] { 1, 3, grid }));
		assertTrue((boolean) m.invoke(i, new Object[] { 2, 1, grid }));
		assertTrue((boolean) m.invoke(i, new Object[] { 2, 2, grid }));
		assertFalse((boolean) m.invoke(i, new Object[] { 2, 3, grid }));
		assertTrue((boolean) m.invoke(i, new Object[] { 3, 1, grid }));
		assertTrue((boolean) m.invoke(i, new Object[] { 3, 2, grid }));
		assertFalse((boolean) m.invoke(i, new Object[] { 3, 3, grid }));
	}

	@Test
	public void testwritePointsIntoArray() throws Exception {
		Class<?> c = Class
				.forName("jsoper.pair.s09hadoop.PointCleaner$ZoneCleanReducer");
		Object i = c.newInstance();
		int[][] grid = new int[30][30];

		Method m = c.getDeclaredMethod("writePointsIntoArray", new Class[] {
				Text.class, int.class, int.class, int[][].class });
		m.setAccessible(true);
		m.invoke(i, new Object[] { new Text("5_7"), 0, 0, grid });
		assertEquals(grid[7][5], 1);

		m.invoke(i, new Object[] { new Text("36_38"), 1, 1, grid });
		assertEquals(grid[8][6], 1);

		m.invoke(i, new Object[] { new Text("67_38"), 2, 1, grid });
		assertEquals(grid[8][7], 1);
	}

	@Test
	public void testDeleteIsolatedPoints() throws Exception {
		Class<?> c = Class
				.forName("jsoper.pair.s09hadoop.PointCleaner$ZoneCleanReducer");
		Object i = c.newInstance();

		int[][] grid = { { 1, 0, 0, 0, 0, 0, 0 }, { 0, 0, 0, 0, 0, 0, 0 },
				{ 0, 0, 0, 0, 0, 0, 0 }, { 0, 0, 0, 1, 0, 0, 0 },
				{ 0, 0, 0, 0, 0, 0, 0 }, { 0, 0, 0, 0, 1, 1, 0 },
				{ 0, 0, 0, 0, 0, 0, 0 } };
		Method m = c.getDeclaredMethod("deleteIsolatedPoints",
				new Class[] { int[][].class });
		m.setAccessible(true);
		m.invoke(i, new Object[] { grid });
		assertEquals(grid[0][0], 1); // on edge so not deleted
		assertEquals(grid[5][4], 1); // the following two keep each other alive
		assertEquals(grid[5][5], 1);
		assertEquals(grid[3][3], 0); // no neighbors so deleted
	}

}
