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
 *           Part_2_Problem_4: Analysing the most occupied time of the day
 *           during the week over the years
 * 
 * @Input bina_classschedule2.csv
 * @Output <<Semester, Day of Week>, Most Busy Time>
 *
 **/
public class LectureTimeAnalysis {
	private static final String MONDAY = "Monday";
	private static final String TUESDAY = "Tuesday";
	private static final String WEDNESDAY = "Wednesday";
	private static final String THURSDAY = "Thursday";
	private static final String FRIDAY = "Friday";

	/**
	 * @param Object
	 *            is the line number of the input file
	 * @param Text
	 *            is the value at the above line number
	 * @param Text
	 *            is the key for output from Mapper1
	 * @param IntWritable
	 *            is the value for output from Mapper1
	 * 
	 * @OutputFormat <key_semester_lecture_day, value_lecture_count>
	 * @Output <Semester_LectureTime_DayOfWeek, LectureCount>
	 * 
	 */
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

		private Text key_semester_lecture_day = new Text();
		private IntWritable value_lecture_count;

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			try {
				String[] tokens = value_at_key.toString().split(",");

				if (tokens.length > 11) {
					return;
				}

				String semester = tokens[3];
				String lecture_time = tokens[7];
				String lecture_days = tokens[6];

				if ("Unknown".contains(lecture_time) || "Unknown".contains(lecture_days)) {
					return;
				}

				if ("UNKWN".contains(lecture_time) || "UNKWN".contains(lecture_days)) {
					return;
				}

				if ("Arr".contains(lecture_time) || "Arr".contains(lecture_days)) {
					return;
				}

				if ("ARR".contains(lecture_time) || "ARR".contains(lecture_days)) {
					return;
				}

				char[] arr_lecture_days = lecture_days.toCharArray();
				String day_of_week;

				for (char lecture_day : arr_lecture_days) {

					if ('M' == lecture_day) {
						day_of_week = MONDAY;

					} else if ('T' == lecture_day) {
						day_of_week = TUESDAY;

					} else if ('W' == lecture_day) {
						day_of_week = WEDNESDAY;

					} else if ('R' == lecture_day) {
						day_of_week = THURSDAY;

					} else if ('F' == lecture_day) {
						day_of_week = FRIDAY;

					} else {
						continue;
					}

					String semester_lecture_day = semester + "_" + lecture_time + "_" + day_of_week;

					key_semester_lecture_day.set(semester_lecture_day);
					value_lecture_count = new IntWritable(1);

					context.write(key_semester_lecture_day, value_lecture_count);

				}

			} catch (NumberFormatException e) {
				System.out.println("NumberFormatException occured... Don't worry");
			}
		}
	}

	/**
	 * @param Text
	 *            is the key from Mapper1 output
	 * @param IntWritable
	 *            is the Iterable value from Mapper1 output
	 * @param Text
	 *            is the key for output from Reducer1
	 * @param IntWritable
	 *            is the value for output from Reducer1
	 * 
	 * @OutputFormat <key_semester_lecture_day, value_total_lecture_count>
	 * @Output <Semester_LectureTime_DayOfWeek, TotalLectureCount>
	 * 
	 */
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable value_total_lecture_count;

		public void reduce(Text key_semester_lecture_day, Iterable<IntWritable> value_iterable_lecture_count,
				Context context) throws IOException, InterruptedException {

			int total_lecture_count = 0;
			for (IntWritable lecture_count : value_iterable_lecture_count) {
				total_lecture_count += lecture_count.get();
			}

			value_total_lecture_count = new IntWritable(total_lecture_count);
			context.write(key_semester_lecture_day, value_total_lecture_count);
		}
	}

	/**
	 * @param Object
	 *            is the line number of Reducer1 output file (stored in temp
	 *            folder)
	 * @param Text
	 *            is the value at the above line number
	 * @param Text
	 *            is the key for output from Mapper2
	 * @param IntWritable
	 *            is the value for output from Mapper2
	 * 
	 * @OutputFormat <key_semester_day, value_lecture_time_total_lecture_count>
	 * @Output <Semester_DayOfWeek, LectureTime_TotalLectureCount>
	 * 
	 */
	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		private Text key_semester_day = new Text();
		private Text value_lecture_time_total_lecture_count = new Text();

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			String[] fields = value_at_key.toString().split("\\t");

			String[] semester_lecture_day = fields[0].split("_");
			String semester_day = semester_lecture_day[0] + "_" + semester_lecture_day[2];

			String lecture_time_total_lecture_count = semester_lecture_day[1] + "_" + fields[1];

			key_semester_day.set(semester_day);
			value_lecture_time_total_lecture_count.set(lecture_time_total_lecture_count);

			context.write(key_semester_day, value_lecture_time_total_lecture_count);
		}
	}

	/**
	 * @param Text
	 *            is the key from Mapper2 output
	 * @param Text
	 *            is the Iterable value from Mapper2 output
	 * @param Text
	 *            is the key for output from Reducer2
	 * @param Text
	 *            is the value for output from Reducer2
	 * 
	 * @OutputFormat <key_semester_day, value_most_busy_time>
	 * @Output <Semester_DayOfWeek, MostBusyTime>
	 * 
	 */
	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		private Text value_most_busy_time = new Text();

		public void reduce(Text key_semester_day, Iterable<Text> value_iterable_lecture_time_total_lecture_count,
				Context context) throws IOException, InterruptedException {

			String most_busy_time = "";
			int highest_lectures = 0;

			for (Text lecture_time_total_lecture_count : value_iterable_lecture_time_total_lecture_count) {
				String[] arr_lecture_time_total_lecture_count = lecture_time_total_lecture_count.toString().split("_");

				int total_lecture_count = Integer.parseInt(arr_lecture_time_total_lecture_count[1]);

				if (highest_lectures < total_lecture_count) {
					highest_lectures = total_lecture_count;
					most_busy_time = arr_lecture_time_total_lecture_count[0];
				}
			}

			String[] arr_semester_day = key_semester_day.toString().split("_");
			String semester_day = arr_semester_day[0] +"\t" + arr_semester_day[1];

			key_semester_day.set(semester_day);
			value_most_busy_time.set(most_busy_time);

			context.write(key_semester_day, value_most_busy_time);
		}
	}

	public static void main(String[] args) throws Exception {
		String temp = "Temp";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Get total number of classes held during the day for each semester");
		job.setJarByClass(LectureTimeAnalysis.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Get most busy time of the day for each semester");
		job2.setJarByClass(LectureTimeAnalysis.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("Temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}