import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Ramanpreet Singh Khinda (rkhinda | 5016-9622)
 * @author Elroy Preetham Alva (elroypre | 5016-8107)
 * 
 * @category Project: Parallel Processing of Big Data using Hadoop MapReduce and
 *           build a Dashboard for Analysis and Visualization of Results
 *
 *           Problem 1: Parallelizing data processing using MR
 *
 **/
public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static IntWritable intValue;
		private Text word = new Text();

		/**
		 * Mapper Function
		 * 
		 * @output: <key, value> pair: (Key=<”Knox_Spring2016”>,Value=6000)
		 * 
		 *          Output for this problem is all the rooms with years and
		 *          capacity served. This is done by reading a line, selecting
		 *          the three needed tokens (room, year and capacity), setting
		 *          the key with (room concatenated with year) and setting the
		 *          value (capacity) and output the <key,value> pair
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				// Since the input file is csv so splitting on based on
				// commas(,)
				String[] tokens = value.toString().split(",");

				// Processing the building, lecture room, semester session and
				// capacity of class
				String building_lecture_room = tokens[2];
				String[] buildings = building_lecture_room.split(" ");
				String building = buildings[0];

				String semester_session = tokens[1];

				/**
				 * Performing cleaning of data to filter out extraneous results
				 */

				// Removing columns with more than 9 columns, since those might
				// not reveal the expected results
				if (tokens.length > 9) {
					return;
				}

				// Filtering out buildings whose details are not known
				if (null == building) {
					return;
				}

				if ("Unknown".contains(building)) {
					return;
				}

				if ("UNKWN".contains(building)) {
					return;
				}

				if ("Arr".contains(building)) {
					return;
				}

				/**
				 * Generating the required Key-Value pair and providing the
				 * output for to be used for Reducer in later processing
				 */
				String myKey = building.concat("_").concat(semester_session);
				int students_enrolled = Integer.parseInt(tokens[7]);

				intValue = new IntWritable(students_enrolled);
				word.set(myKey);

				context.write(word, intValue);
			} catch (NumberFormatException e) {
				System.out.println("NumberFormatException occured... Dont't worry");
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}