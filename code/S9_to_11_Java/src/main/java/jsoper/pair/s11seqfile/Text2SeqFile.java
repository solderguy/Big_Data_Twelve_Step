package jsoper.pair.s11seqfile;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Text2SeqFile {
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
				// System.out.println("word3: " + word3.toString() + " word4: "
				// + word4.toString());

				context.write(wordCl, wordXY);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		// delete output folder
		File outputFolder = new File("out_s11");
		FileUtils.deleteDirectory(outputFolder);
		outputFolder = null;

		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);

		// increase if you need sorting or a special number of files
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("out_s10"));
		FileOutputFormat.setOutputPath(job, new Path("out_s11"));

		job.waitForCompletion(true);
		System.out.println("done with Step 11");

	}
}
