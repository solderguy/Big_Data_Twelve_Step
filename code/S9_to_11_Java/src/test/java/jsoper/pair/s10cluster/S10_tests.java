package jsoper.pair.s10cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class S10_tests {
	static KMeansClustering tester = null;
	
	// Simplify test setup with some basic ivars to use globally (have name g*) 
	static Configuration gConf = null;
	static FileSystem gFs = null;
	static double[][] gTestPoints = new double[2][2];
	static List<Vector> gTestVectors = new ArrayList<Vector>();

	@BeforeClass
	public static void oneTimeSetUp() throws IOException {
		tester = new KMeansClustering();

		gConf = new Configuration();
		gFs = FileSystem.get(gConf);
		
		gTestPoints[0][0] = 17.0;
		gTestPoints[0][1] = 14.0;
		gTestPoints[1][0] = 5.0;
		gTestPoints[1][1] = 8.0;
		
		for (int i = 0; i < gTestPoints.length; i++) {
			double[] fr = gTestPoints[i];
			Vector vec = new RandomAccessSparseVector(fr.length);
			vec.assign(fr);
			gTestVectors.add(vec);
		}	
	}

	
	@Test
	public void testcreateDataFolderIfNeeded() throws IOException {
		String dirName = "testdata";
		File dir = new File(dirName);
		FileUtil.fullyDelete(dir);

		boolean exists = dir.exists();
		assertFalse(exists);

		tester.createDataFolderIfNeeded();
		exists = dir.exists();
		assertTrue(exists);

		dir = new File(dirName + "/points");
		exists = dir.exists();
		assertTrue(exists);
	}

	
	@Test
	public void testWriteCsvData() throws IOException {
		String outputFile = "test_output_file.txt";
		String textString = "data1, data2, data3";
		tester.writeCsvOutputFile(textString, outputFile);

		try (BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
			String line = br.readLine();
			assertEquals(line, textString);
			FileUtil.fullyDelete(new File(outputFile));
		}
	}
	
	@Test
	public void testReadCsvOutputFile() {
		double[][] testPoints = new double[2][2];
		String outputFile = "test_output_file.txt";
		String textString = "70,19,4\n42,25,1\n";

		// possible false failure if writeCsvOutput has problem and not tested first
		tester.writeCsvOutputFile(textString, outputFile);
		tester.readCsvData(testPoints, outputFile);
		assertEquals(testPoints[0][0], 70, 0.01);
		assertEquals(testPoints[0][1], 19, 0.01);
		assertEquals(testPoints[1][0], 42, 0.01);
		assertEquals(testPoints[1][1], 25, 0.01);
	}
		
	@Test
	public void testVectorize() {
		List<Vector> testVectors = tester.vectorize(gTestPoints);
		int size = testVectors.size();
		assertEquals(size, 2);

		Vector v0 = testVectors.get(0);
		Vector v1 = testVectors.get(1);
		assertEquals(v0.maxValue(), 17.0, 0.01);
		assertEquals(v0.minValue(), 14.0, 0.01);
		assertEquals(v1.maxValue(), 8.0, 0.01);
		assertEquals(v1.minValue(), 5.0, 0.01);
	}
		
	@Test
	public void testwritePointsToSeqFile() throws IOException {
		String fileName = "outputFile";
		File file = new File(fileName);
		FileUtil.fullyDelete(file);
		boolean exists = file.exists();
		assertFalse(exists);

		tester.writePointsToSeqFile(gTestVectors, fileName, gFs, gConf);
		exists = file.exists();
		assertTrue(exists);

		long size = file.length();
		assertEquals(size, 172);

		FileUtil.fullyDelete(file);
	}
	
	@Test
	public void testwriteInitialClusterCenters() throws IOException {
		String fileName = "testdata/clusters/part-00000";
		File file = new File(fileName);
		FileUtil.fullyDelete(file);
		boolean exists = file.exists();
		assertFalse(exists);
		
		int k = 2;
		tester.writeInitialClusterCenters(gConf, gFs, k, gTestVectors);
		exists = file.exists();
		assertTrue(exists);
		
		long size = file.length();
		assertEquals(size, 316);
		FileUtil.fullyDelete(file);
	}

	@AfterClass
	public static void oneTimeTearDown() {
		tester = null;
	}

}
