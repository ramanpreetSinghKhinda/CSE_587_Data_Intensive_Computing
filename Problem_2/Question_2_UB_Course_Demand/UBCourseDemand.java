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
 *           Part_2_Problem_2: Analyzing the course demand at UB till now by
 *           taking the average of enrolled students and class capacity
 * 
 * @Input bina_classschedule2.csv
 * @Output <Course Name, <Department, Average Enrolled Students, Average Class
 *         Capacity, Demand>>
 *
 **/
public class UBCourseDemand {
	private static final String HIGH_DEMAND = "High Demand";
	private static final String ABOVE_AVERAGE_DEMAND = "Above Average Demand";
	private static final String AVERAGE_DEMAND = "Average Demand";
	private static final String BELOW_AVERAGE_DEMAND = "Below Average Demand";
	private static final String NO_DEMAND = "No Demand";

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
	 * @OutputFormat <key_course_dept, value_enrolled_capacity>
	 * @Output <CourseName_DepartmentName, StudentsEnrolled_ClassCapacity>
	 * 
	 */
	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private Text key_course_dept = new Text();
		private Text value_enrolled_capacity = new Text();

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			try {
				// Since the input file is csv so splitting on based on
				// commas(,)
				String[] tokens = value_at_key.toString().split(",");

				if (tokens.length > 11) {
					return;
				}

				// Processing the course name, enrolled student and capacity of
				// class
				String course_name = tokens[8];
				String dept_name = tokens[4];

				int students_enrolled = Integer.parseInt(tokens[9]);
				int class_cap = Integer.parseInt(tokens[10]);

				if (0 == class_cap) {
					return;
				}

				String enrolled_capacity = students_enrolled + "_" + class_cap;

				key_course_dept.set(course_name + "_" + dept_name);
				value_enrolled_capacity.set(enrolled_capacity);

				context.write(key_course_dept, value_enrolled_capacity);
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
	 * @OutputFormat <key_course_dept, value_total_enrolled_capacity>
	 * @Output <CourseName_DepartmentName,
	 *         TotalEnrollment_TotalCapacity_NumOfSemestersCourseOffered>
	 * 
	 */
	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		private Text value_total_enrolled_capacity = new Text();

		public void reduce(Text key_course_dept, Iterable<Text> value_iterable_enrolled_capacity, Context context)
				throws IOException, InterruptedException {
			int total_enrollment = 0;
			int total_capacity = 0;
			int num_of_semesters_course_offered = 0;

			String total_enrolled_capacity;

			for (Text enrolled_capacity : value_iterable_enrolled_capacity) {
				String[] arr_enrolled_capacity = enrolled_capacity.toString().split("_");
				int students_enrolled = Integer.parseInt(arr_enrolled_capacity[0]);
				int class_cap = Integer.parseInt(arr_enrolled_capacity[1]);

				total_enrollment += students_enrolled;
				total_capacity += class_cap;

				++num_of_semesters_course_offered;
			}

			total_enrolled_capacity = total_enrollment + "_" + total_capacity + "_" + num_of_semesters_course_offered;

			value_total_enrolled_capacity.set(total_enrolled_capacity);
			context.write(key_course_dept, value_total_enrolled_capacity);
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
	 * @OutputFormat <key_course_dept, value_total_enrolled_capacity>
	 * @Output <CourseName_DepartmentName,
	 *         TotalEnrollment_TotalCapacity_NumOfSemestersCourseOffered>
	 */
	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		private Text key_course_dept = new Text();
		private Text value_total_enrolled_capacity = new Text();

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			String[] fields = value_at_key.toString().split("\\t");

			String course_dept = fields[0];
			String total_enrolled_capacity = fields[1];

			key_course_dept.set(course_dept);
			value_total_enrolled_capacity.set(total_enrolled_capacity);

			context.write(key_course_dept, value_total_enrolled_capacity);
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
	 * @OutputFormat <key_course_name, value_avg_enrolled_capacity_demand>
	 * @Output <CourseName, DepartmentName_AverageEnrollment_AverageCapacity_Demand>
	 */
	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		private Text key_course_name = new Text();
		private Text value_avg_enrolled_capacity_demand = new Text();

		public void reduce(Text key_course_dept, Iterable<Text> value_iterable_total_enrolled_capacity, Context context)
				throws IOException, InterruptedException {
			int total_enrollment = 0;
			int total_capacity = 0;
			int num_of_semesters_course_offered = 0;

			int avg_enrollment = 0;
			int avg_capacity = 0;

			String avg_enrolled_capacity;

			for (Text total_enrolled_capacity : value_iterable_total_enrolled_capacity) {
				String[] arr_total_enrolled_capacity = total_enrolled_capacity.toString().split("_");

				total_enrollment += Integer.parseInt(arr_total_enrolled_capacity[0]);
				total_capacity += Integer.parseInt(arr_total_enrolled_capacity[1]);
				num_of_semesters_course_offered += Integer.parseInt(arr_total_enrolled_capacity[2]);
			}

			avg_enrollment = total_enrollment / num_of_semesters_course_offered;
			avg_capacity = total_capacity / num_of_semesters_course_offered;

			String demand = AVERAGE_DEMAND;
			if (avg_enrollment >= avg_capacity) {
				demand = HIGH_DEMAND;

			} else {
				float percent_vacant = (float) (((avg_capacity - avg_enrollment) * 100.0) / avg_capacity);

				if (percent_vacant < 10) {
					demand = HIGH_DEMAND;

				} else if (percent_vacant < 25) {
					demand = ABOVE_AVERAGE_DEMAND;

				} else if (percent_vacant < 40) {
					demand = AVERAGE_DEMAND;

				} else if (percent_vacant < 70) {
					demand = BELOW_AVERAGE_DEMAND;

				} else {
					demand = NO_DEMAND;
				}
			}

			String[] course_dept = key_course_dept.toString().split("_");
			avg_enrolled_capacity = course_dept[1] + "\t" + avg_enrollment + "\t" + avg_capacity + "\t" + demand;

			key_course_name.set(course_dept[0]);
			value_avg_enrolled_capacity_demand.set(avg_enrolled_capacity);

			context.write(key_course_name, value_avg_enrolled_capacity_demand);
		}
	}

	public static void main(String[] args) throws Exception {
		String temp = "Temp";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,
				"Get total number of enrolled students and total class capacity for each course and department");
		job.setJarByClass(UBCourseDemand.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2,
				"Get average enrolled students and average class capacity for each course and department");
		job2.setJarByClass(UBCourseDemand.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("Temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}