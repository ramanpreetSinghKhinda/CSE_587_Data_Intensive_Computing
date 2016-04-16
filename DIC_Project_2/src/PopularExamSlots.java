import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 *           Part_2_Problem_4: Finding the most popular exam slot in every building
 * 
 * @Input bina_examschedule.tsv
 * @Output <BuildingName, TimeSlot>
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
	 * @OutputFormat <key_building_lecture_room, value_enrolled_capacity>
	 * @Output <BuildingName_LectureRoom, StudentsEnrolled_ClassCapacity>
	 * 
	 */
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
		private Text key_building_lecture_room = new Text();
		private Text value_enrolled_capacity = new Text();

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			try {
				String[] tokens = value_at_key.toString().split("\\t");
				if (tokens.length != 14) {
					return;
				}

				String building = tokens[3].split(" ")[0];
				context.write(key_building_lecture_room, value_enrolled_capacity);
			} catch (NumberFormatException e) {
				System.out.println("NumberFormatException occured... Don't worry");
			}
		}
	}

	/**
	 * @param Text
	 *            is the key from Mapper1 output
	 * @param Text
	 *            is the Iterable value from Mapper1 output
	 * @param Text
	 *            is the key for output from Reducer1
	 * @param Text
	 *            is the value for output from Reducer1
	 * 
	 * @OutputFormat <key_building_lecture_room, value_total_enrolled_capacity>
	 * @Output <BuildingName_LectureRoom, TotalEnrollment_TotalCapacity>
	 * 
	 */
	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		private Text value_total_enrolled_capacity = new Text();

		public void reduce(Text key_building_lecture_room, Iterable<Text> value_iterable_enrolled_capacity,
				Context context) throws IOException, InterruptedException {
			int total_enrollment = 0;
			int total_capacity = 0;

			for (Text enrolled_capacity : value_iterable_enrolled_capacity) {
				if (!enrolled_capacity.toString().split("-")[0].equals("")
						&& !enrolled_capacity.toString().split("-")[1].equals("")) {
					total_enrollment += Integer.parseInt(enrolled_capacity.toString().split("-")[0]);
					total_capacity += Integer.parseInt(enrolled_capacity.toString().split("-")[1]);

				} else {
					return;
				}
			}

			value_total_enrolled_capacity.set(total_enrollment + "-" + total_capacity);
			context.write(key_building_lecture_room, value_total_enrolled_capacity);
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
	 * @OutputFormat <key_building_name, value_total_enrolled_capacity>
	 * @Output <BuildingName, TotalEnrollment_TotalCapacity>
	 */
	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		Text key_building_name = new Text();
		Text value_total_enrolled_capacity = new Text();

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			String[] fields = value_at_key.toString().split("\\t");

			String building_name = fields[0].split(" ")[0];

			key_building_name.set(building_name);
			value_total_enrolled_capacity.set(fields[1]);

			context.write(key_building_name, value_total_enrolled_capacity);
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
	 * @OutputFormat <key_building_name, value_percent_vacant>
	 * @Output <BuildingName, PercentVacant>
	 */
	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		Text value_percent_vacant = new Text();

		public void reduce(Text key_building_name, Iterable<Text> value_iterable_total_enrolled_capacity,
				Context context) throws IOException, InterruptedException {
			int total_enrollment = 0;
			int total_capacity = 0;

			for (Text total_enrolled_capacity : value_iterable_total_enrolled_capacity) {
				int enrollment = Integer.parseInt(total_enrolled_capacity.toString().split("-")[0]);
				int capacity = Integer.parseInt(total_enrolled_capacity.toString().split("-")[1]);

				total_enrollment += enrollment;
				total_capacity += capacity;
			}

			int percent_vacant = ((total_capacity - total_enrollment) * 100) / total_capacity;

			value_percent_vacant.set(percent_vacant + "% of this buildings' capacity has not been used over the years");
			context.write(key_building_name, value_percent_vacant);
		}
	}

	public static void main(String[] args) throws Exception {
		String temp = "Temp";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Get number of students enrolled per lecture hall");
		job.setJarByClass(PopularExamSlots.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Get percent vacancy per building");
		job2.setJarByClass(PopularExamSlots.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("Temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}