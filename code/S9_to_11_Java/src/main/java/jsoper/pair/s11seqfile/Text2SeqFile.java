package jsoper.pair.s11seqfile;
/**
 * This program is the eleventh part of the Big Data 12 Step Program
 *
 * It reads the text file produced by Mahout and writes the data
 * out in Sequence File format
 *  
 * @author John Soper
 *
 */


import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Text2SeqFile extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text wordX = new Text();
		private Text wordY = new Text();
		private Text wordCl = new Text();
		private Text wordXY = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			while (tokenizer.hasMoreTokens()) {
				wordX.set(tokenizer.nextToken());
				wordY.set(tokenizer.nextToken());
				wordCl.set(tokenizer.nextToken());
				wordXY.set(wordX.toString() + "," + wordY.toString());
				context.write(wordCl, wordXY);
			}
		}
	}

	public int run(String[] args) throws Exception {

		// delete output folder
		File outputFolder = new File("out_s11");
		FileUtils.deleteDirectory(outputFolder);
		outputFolder = null;

		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);

		// increase if you need key sorting or a specific number of output files
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("out_s10"));
		FileOutputFormat.setOutputPath(job, new Path("out_s11"));

		job.waitForCompletion(true);
		return 1;
	}
	
	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new Text2SeqFile(), args);
		System.exit(exitCode);
	}
}