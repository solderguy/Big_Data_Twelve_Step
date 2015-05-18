package jsoper.pair.s10cluster;

/**
 * This program is the tenth part of the Big Data 12 Step Program
 *
 * It performs a K-means clustering on the bitmap with k=7, which
 * is one cluster per letter
 *
 * @author John Soper
 *
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class KMeansClustering extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(KMeansClustering.class);

	void writePointsToSeqFile(List<Vector> points, String fileName, FileSystem fs,
			Configuration conf) {
		Path path = new Path(fileName);
		try (SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				path, LongWritable.class, VectorWritable.class)) {
			long recNum = 0;
			VectorWritable vec = new VectorWritable();
			for (Vector point : points) {
				vec.set(point);
				writer.append(new LongWritable(recNum++), vec);
			}
		} catch (IOException e) {
			System.out.println("writePointsToFile IO exception");
			e.printStackTrace();
		}
		LOG.info("size vectors: " + points.size());
	}

	List<Vector> vectorize(double[][] raw) {
		List<Vector> points = new ArrayList<Vector>();
		for (int i = 0; i < raw.length; i++) {
			double[] fr = raw[i];
			Vector vec = new RandomAccessSparseVector(fr.length);
			vec.assign(fr);
			points.add(vec);
		}
		LOG.info("vectors wptf.size: " + points.size());
		return points;
	}

//	String getCorrectLetter(double x, double y) {
//		if (y > 60) {
//			if (x < 45)
//				return "B";
//			else if (x > 72)
//				return "G";
//			else
//				return "I";
//		} else {
//			if (x < 40)
//				return "D";
//			else if (x < 60)
//				return "A1";
//			else if (x < 80)
//				return "T";
//			else
//				return "A2";
//		}
//	}

	void chooseInitialCenterValues(double[][] initCtrs) {
		initCtrs[0][0] = 40;
		initCtrs[0][1] = 100;
		initCtrs[1][0] = 60;
		initCtrs[1][1] = 100;
		initCtrs[2][0] = 100;
		initCtrs[2][1] = 100;
		initCtrs[3][0] = 20;
		initCtrs[3][1] = 20;
		initCtrs[4][0] = 50;
		initCtrs[4][1] = 20;
		initCtrs[5][0] = 75;
		initCtrs[5][1] = 20;
		initCtrs[6][0] = 100;
		initCtrs[6][1] = 20;
	}

	void readCsvData(double[][] fpoints, String inputFile) {
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
			String line = br.readLine();
			int row = 0;

			while (line != null) {
				String[] tokens = line.split(",");
				fpoints[row][0] = Double.parseDouble(tokens[0]);
				fpoints[row++][1] = Double.parseDouble(tokens[1]);
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			System.out.println("readInputData file not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("readInputData IO exception");
			e.printStackTrace();
		}
	}

	void writeInitialClusterCenters(Configuration conf, FileSystem fs, int k,
			List<Vector> initialClusterVectors) {
		Path path = new Path("testdata/clusters/part-00000");
		try (SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				path, Text.class, Cluster.class)) {
			for (int i = 0; i < k; i++) {
				Vector vec = initialClusterVectors.get(i);
				LOG.info("vec: " + vec);
				Cluster cluster = new Cluster(vec, i,
						new EuclideanDistanceMeasure());
				writer.append(new Text(cluster.getIdentifier()), cluster);
			}
		} catch (IOException e) {
			System.out.println("writeInitialClusterCenters IO Exception");
			e.printStackTrace();
		}
	}

	void runKMeansAlgorithm(Configuration conf, FileSystem fs) {
		try {
			KMeansDriver.run(conf, new Path("testdata/points"), new Path(
					"testdata/clusters"), new Path("output"),
					new EuclideanDistanceMeasure(), 0.001, 10, true, false);
		} catch (ClassNotFoundException e) {
			System.out.println("runKmeans IO Class Not Found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("runKmeans IO Exception");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("runKmeans Interrupted E");
			e.printStackTrace();
		}
	}
	
	String readSingleClusterValue(IntWritable key, WeightedVectorWritable value) {
		StringBuilder sb = new StringBuilder(value.toString());
		sb.deleteCharAt(sb.length() - 1);
		sb.deleteCharAt(sb.indexOf(" "));
		sb.delete(0, 5);

		String[] tokens = sb.toString().split(",");
		double x = Double.MAX_VALUE; // want absurd value here
		double y = Double.MAX_VALUE; // to prevent false pass
		try {
			x = Double.parseDouble(tokens[0]);
			y = Double.parseDouble(tokens[1]);
		} catch (Exception e) {
			System.out
					.println("\n\n****** S10 parsing error on value to double\n\n");
			e.printStackTrace();
		}
		LOG.info(value.toString() + " belongs to cluster " + key.toString());
		
		//LOG.info(value.toString() + " belongs to cluster " + key.toString()
		//	+ " Letter: " + getCorrectLetter(x, y));
		return ("" + x + "," + y + "," + key.toString() + "\n");
	}

	String readAllClusterValues(Configuration conf, FileSystem fs) {
		StringBuilder sb = new StringBuilder("x,y,cluster\n");
		try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
				"output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-00000"),
				conf)) {
			IntWritable key = new IntWritable();
			WeightedVectorWritable value = new WeightedVectorWritable();
			while (reader.next(key, value)) {
				sb.append(readSingleClusterValue(key, value));
			}
		} catch (IOException e) {
			System.out
					.println("problem allocating reader sequence file reader");
			e.printStackTrace();
		}
		return (sb.toString());
	}

	void createDataFolderIfNeeded() {
		File testData = new File("testdata");
		if (!testData.exists()) {
			testData.mkdir();
		}
		testData = new File("testdata/points");
		if (!testData.exists()) {
			testData.mkdir();
		}
	}

	FileSystem allocateFileSystem(Configuration conf) {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			System.out.println("Could not allocate file system");
			e.printStackTrace();
		}
		return fs;
	}

	void writeCsvOutputFile(String str, String outputFile) {
		try {
			Files.write(Paths.get(outputFile), str.getBytes());
		} catch (IOException e) {
			System.out.println("Problem writing s10out.csv");
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		// change below dimensions to command line arguments if
		// more general behavior is ever needed
		// chooseInitialCenterValues also has hardcoded values
		int k = 7; // number of clusters
		double[][] dataPoints = new double[168][2];
		double[][] initialClusCenters = new double[7][2];

		// housekeeping
		createDataFolderIfNeeded();
		Configuration conf = new Configuration();
		FileSystem fs = allocateFileSystem(conf);

		// transfer data points from csv file to sequence file
		readCsvData(dataPoints, args[0]);
		List<Vector> dataVectors = vectorize(dataPoints);
		writePointsToSeqFile(dataVectors, "testdata/points/file1", fs, conf);

		// set beginning cluster values
		chooseInitialCenterValues(initialClusCenters);
		List<Vector> initialClusterVectors = vectorize(initialClusCenters);
		writeInitialClusterCenters(conf, fs, k, initialClusterVectors);

		// perform k-means algorithm
		runKMeansAlgorithm(conf, fs);

		// Write points and clusters IDs to CSV file
		String str = new String(readAllClusterValues(conf, fs));
		writeCsvOutputFile(str, args[1]);
		
		return 1;
	}

	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new KMeansClustering(), args);
		System.exit(exitCode);
	}

}
