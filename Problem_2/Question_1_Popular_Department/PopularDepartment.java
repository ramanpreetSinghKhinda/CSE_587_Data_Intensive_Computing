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
 *           Part_2_Problem_1: Finding the department with most enrolled
 *           students every year
 * 
 * @Input bina_classschedule2.csv
 * @Output <Year, <Department, Highest Enrolled Students>>
 *
 **/
public class PopularDepartment {

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
	 * @OutputFormat <key_year_dept, value_students_enrolled>
	 * @Output <Year_DepartmentName, StudentsEnrolled>
	 * 
	 */
	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

		private Text key_year_dept = new Text();
		private static IntWritable value_students_enrolled;

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			try {
				String[] tokens = value_at_key.toString().split(",");
				if (tokens.length > 11) {
					return;
				}

				String semester_session = tokens[3];
				String dept = tokens[4];
				int students_enrolled = Integer.parseInt(tokens[9]);

				String[] semester = semester_session.split(" ");
				String year = semester[1];

				String year_dept = year.concat("-").concat(dept);

				key_year_dept.set(year_dept);
				value_students_enrolled = new IntWritable(students_enrolled);

				context.write(key_year_dept, value_students_enrolled);
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
	 * @OutputFormat <key_year_dept, value_total_students_enrolled>
	 * @Output <Year_DepartmentName, TotalStudentsEnrolled>
	 * 
	 */
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable value_total_students_enrolled = new IntWritable();

		public void reduce(Text key_year_dept, Iterable<IntWritable> value_iterable_students_enrolled, Context context)
				throws IOException, InterruptedException {

			int total_students_enrolled = 0;
			for (IntWritable students_enrolled : value_iterable_students_enrolled) {
				total_students_enrolled += students_enrolled.get();
			}

			value_total_students_enrolled.set(total_students_enrolled);
			context.write(key_year_dept, value_total_students_enrolled);
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
	 * @OutputFormat <key_year, value_department_total_students_enrolled>
	 * @Output <Year, <DepartmentName, TotalStudentsEnrolled>>
	 */
	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		Text key_year = new Text();
		Text value_department_total_students_enrolled = new Text();

		public void map(Object key_line_number, Text value_at_key, Context context)
				throws IOException, InterruptedException {
			String[] fields = value_at_key.toString().split("\\t");

			String year = fields[0].split("-")[0];
			String department_total_students_enrolled = fields[0].split("-")[1] + "-" + fields[1];

			key_year.set(year);
			value_department_total_students_enrolled.set(department_total_students_enrolled);

			context.write(key_year, value_department_total_students_enrolled);
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
	 * @OutputFormat <key_year_most_demanding_dept,
	 *               value_highest_enrolled_students>
	 * @Output <Year_MostDemandingDept, HighestEnrolledStudents>
	 * 
	 */
	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		Text key_year_most_demanding_dept = new Text();
		Text value_highest_enrolled_students = new Text();

		public void reduce(Text key_year, Iterable<Text> value_iterable_department_total_students_enrolled,
				Context context) throws IOException, InterruptedException {
			String most_demanding_dept = "";
			int highest_enrolled_students = 0;

			for (Text department_total_students_enrolled : value_iterable_department_total_students_enrolled) {
				String dept = department_total_students_enrolled.toString().split("-")[0];

				int total_students_enrolled = Integer
						.parseInt(department_total_students_enrolled.toString().split("-")[1]);

				if (total_students_enrolled > highest_enrolled_students) {
					most_demanding_dept = dept;
					highest_enrolled_students = total_students_enrolled;
				}
			}

			if("".equalsIgnoreCase(most_demanding_dept) || 0 == highest_enrolled_students) {
				return;
			}
			
			String year_most_demanding_dept = key_year.toString() + "\t" + most_demanding_dept;

			key_year_most_demanding_dept.set(year_most_demanding_dept);
			value_highest_enrolled_students.set(highest_enrolled_students + "");

			context.write(key_year_most_demanding_dept, value_highest_enrolled_students);
		}
	}

	public static void main(String[] args) throws Exception {
		String temp = "Temp";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Get number of students per department per year");
		job.setJarByClass(PopularDepartment.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Get department with highest students per year");
		job2.setJarByClass(PopularDepartment.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("Temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}