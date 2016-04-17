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
 *           Part_2_Problem_4: Finding most used class in every hall for exam
 *           using time slot
 * 
 * @Input bina_examschedule.tsv
 * @Output <Building, <Hall, No of exams held>>
 *
 **/
public class PopularExamSlots {

	/**
	 * @param Object
	 *            is the line number of the input file
	 * @param Text
	 *            is the value at the above line number
	 * @param Text
	 *            is the key for output from Mapper1
	 * @param Text
	 *            is the value for output from Mapper1
	 * 
	 * @OutputFormat <key_exam_hall_stime, one>
	 * @Output <Exam_Hall, 1>
	 * 
	 */
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private Text key_exam_hall_stime = new Text();
		private Text one = new Text();

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			try {
				String[] tokens = value_at_key.toString().split("\\t");
				if (tokens.length > 13) {
					return;
				}

				if (tokens[0].equals("") || tokens[0].contains("TERM") || tokens[0].contains("----")) {
					return;
				}

				String exam_hall = tokens[4].trim();
				
				if("Arr Arr".contains(exam_hall)) {
					return;
				}

				String stime = tokens[7].trim();

				key_exam_hall_stime.set(exam_hall + "__" + stime);
				one.set("" + 1);

				context.write(key_exam_hall_stime, one);
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
	 * @param Text
	 *            is the value for output from Reducer1
	 * 
	 * @OutputFormat <key_hall, slot_stime>
	 * @Output <Building_Hall, StartTime_Sum>
	 * 
	 */
	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		private Text slot_and_no_of_slots = new Text();
		private Text key_hall = new Text();

		public void reduce(Text key_exam_hall_stime, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (Text val : values) {
				++sum;
			}

			if (key_exam_hall_stime.toString().split("__").length < 2) {
				return;
			}

			key_hall.set(key_exam_hall_stime.toString().split("__")[0]);
			slot_and_no_of_slots.set(key_exam_hall_stime.toString().split("__")[1] + "__" + sum);

			context.write(key_hall, slot_and_no_of_slots);
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
	 * @param Text
	 *            is the value for output from Mapper2
	 * 
	 * @OutputFormat <building_slot, number_of_slots>
	 * @Output <Building, <Slot, no_of_slots>>
	 */
	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
		Text building_slot = new Text();
		IntWritable number_of_slots;

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			String[] fields = value_at_key.toString().split("\\t");

			String building = fields[0].split(" ")[0];
			String slot = fields[1].split("__")[0];

			int no_slots = Integer.parseInt(fields[1].split("__")[1]);

			building_slot.set(building + "\t" + slot);
			number_of_slots = new IntWritable(no_slots);
			context.write(building_slot, number_of_slots);
		}
	}

	/**
	 * @param Text
	 *            is the key from Mapper2 output
	 * @param IntWritable
	 *            is the Iterable value from Mapper2 output
	 * @param Text
	 *            is the key for output from Reducer2
	 * @param IntWritable
	 *            is the value for output from Reducer2
	 * 
	 * @OutputFormat <key_year_most_demanding_dept,
	 *               value_highest_enrolled_students>
	 * @Output <Year_MostDemandingDept, HighestEnrolledStudents>
	 * 
	 */
	public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable no_slots;

		public void reduce(Text building_slot, Iterable<IntWritable> no_of_slots, Context context)
				throws IOException, InterruptedException {

			int slots_in_building = 0;

			for (IntWritable ns : no_of_slots) {
				slots_in_building += ns.get();
			}

			no_slots = new IntWritable(slots_in_building);

			context.write(building_slot, no_slots);
		}
	}

	public static void main(String[] args) throws Exception {
		String temp = "Temp";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Get number of slots per exam hall");
		job.setJarByClass(PopularExamSlots.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Get number of slots per building");
		job2.setJarByClass(PopularExamSlots.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path("Temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}