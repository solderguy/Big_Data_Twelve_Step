package jsoper.pair.s09hadoop;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A MapReduce application to count how many movies an actor has appeared in
 * 
 * Accomplishes the sort order by doing a second job with a second mapper (no
 * reduce)
 * 
 * @author john
 * 
 */
public class PointCleaner extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(PointCleaner.class);
	static int arrayShift = 0;;

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: pointcleaner <in> <out>");
			System.exit(2);
		}
		// delete temp output folder
		String tempOutputString = args[1] + "/temp";
		File tempOutputFolder = new File(tempOutputString);
		FileUtil.fullyDelete(tempOutputFolder);
		tempOutputFolder = null;

		// ConfigurationUtil.dumpConfigurations(conf, System.out);
		LOG.info("input: " + args[0] + " output: " + tempOutputString);

		Job job = new Job(conf, "movie count");
		job.setJarByClass(PointCleaner.class);
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setReducerClass(MovieYearReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(tempOutputString));

		arrayShift = 0; // global ivar
		boolean result = job.waitForCompletion(true);
		LOG.info("****************    Finished first job");
		if (result == false)
			return 1;
		else { // run second job if first succeeds
			String[] run2Args = new String[2];
			run2Args[0] = tempOutputString;
			run2Args[1] = args[1] + "/final";
			int resultJob2 = runJob2(run2Args);
			return resultJob2;
		}
	}

	public int runJob2(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: moviecount <in> <out>");
			System.exit(2);
		}

		// delete final output folder
		File finalOutputFolder = new File(args[1]);
		FileUtil.fullyDelete(finalOutputFolder);
		finalOutputFolder = null;

		// ConfigurationUtil.dumpConfigurations(conf, System.out);
		// LOG.info("input: " + args[0] + " output: " + args[1]);

		Job job = new Job(conf, "movie count");
		job.setJarByClass(PointCleaner.class);
		job.setMapperClass(MovieTokenizerMapper.class);
		job.setReducerClass(MovieYearReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		arrayShift = 15; // global ivar
		boolean result = job.waitForCompletion(true);
		LOG.info("****************    Finished Part 2");
		return (result) ? 0 : 1;
	}

	public static class MovieTokenizerMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text zone = new Text();
		private Text coordinates = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");

			if (tokens.length >= 2) {
				coordinates.set(tokens[0] + "_" + tokens[1]);
				if (tokens[0].toString().equals("x")) {
					zone.set("-1zoneX_zoneY");
				} else
					zone.set(calculateZone(tokens));
				context.write(zone, coordinates);
			}
		}

		private String calculateZone(String[] tokens) {
			double x = -1, y = -1;
			try {
				x = Double.parseDouble(tokens[0]);
				y = Double.parseDouble(tokens[1]);
			} catch (Exception e) {
				System.out
						.println("\n\n****** S9 parsing error on value to double\n\n");
				e.printStackTrace();
			}
			String zone = "-1_-1";
			zone = "" + (int) ((x + arrayShift) / 30) + "_"
					+ (int) ((y + arrayShift) / 30);
			return zone;
		}
	}

	public static class MovieYearReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text outTxt = new Text("placeholder");
		private Text empty = new Text("");
		private int[][] grid = new int[30][30];

		@Override
		public void reduce(Text zone, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			clearArray(grid);

			String[] tokens = zone.toString().replace("\t", "").split("_");
			int zoneX = Integer.parseInt(tokens[0]);
			int zoneY = Integer.parseInt(tokens[1]);

			for (Text val : values) {
				writePointsIntoArray(val, zoneX, zoneY);
			}
			deleteIsolatedPoints();

			StringBuilder sb = new StringBuilder(300);
			int last = grid.length;
			for (int i = 0; i < last; i++) {
				for (int j = 0; j < last; j++) {
					if (grid[j][i] == 1) {
						int origX = 30 * zoneX + i - arrayShift;
						int origY = 30 * zoneY + j - arrayShift;
						sb.append("" + origX + "," + origY + "\n");
					}
				}
			}
			outTxt.set(sb.toString());
			if (outTxt.getLength() > 3) {
				context.write(empty, outTxt);
			}
		}

		private void deleteIsolatedPoints() {
			int last = grid.length - 2; // assume square grid only for now
			// for loops are inset so we don't analyze anything on the borders
			// will get them on the 2nd MR run

			for (int i = 1; i < last; i++) {
				for (int j = 1; j < last; j++) {
					if (grid[j][i] == 1 && hasNeighbor(i, j) == false) {
						grid[j][i] = 0;
					}
				}
			}
		}

		private boolean hasNeighbor(int j, int i) {
			if (grid[i - 1][j] == 1 //
					|| grid[i + 1][j] == 1 //
					|| grid[i - 1][j - 1] == 1 //
					|| grid[i][j - 1] == 1 //
					|| grid[i + 1][j - 1] == 1 //
					|| grid[i - 1][j + 1] == 1 //
					|| grid[i][j + 1] == 1 //
					|| grid[i + 1][j + 1] == 1)
				return true;
			else
				return false;
		}

		private void writePointsIntoArray(Text val, int zoneX, int zoneY) {
			String[] tokens = val.toString().replace("\t", "").split("_");
			int pointX = -1, pointY = -1;

			try {
				pointX = Integer.parseInt(tokens[0]);
				pointY = Integer.parseInt(tokens[1]);
			} catch (Exception e) {
				System.out
						.println("\n\n****** S9 parsing error on value to int\n\n");
				e.printStackTrace();
			}
			grid[pointY + arrayShift - 30 * zoneY][pointX + arrayShift - 30
					* zoneX] = 1;
		}

		private void clearArray(int[][] grid) {
			for (int[] row : grid)
				Arrays.fill(row, 0);
		}

//		private void printArray() {
//			for (int[] row2 : grid) {
//				for (int element : row2) {
//					System.out.print(element);
//				}
//				System.out.println("");
//			}
//			System.out.println("\n\n");
//		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PointCleaner(), args);
		System.exit(exitCode);
	}
}