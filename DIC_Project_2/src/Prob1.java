import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Prob1 {

	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

		private static IntWritable intValue;
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] tokens = value.toString().split(",");
				if (tokens.length > 11) {
					return;
				}

				String semester_session = tokens[3];
				String dept = tokens[4];
				String building_lecture_room = tokens[5];
				String class_days = tokens[6];
				String lecture_time = tokens[7];
				String course = tokens[8];
				int students_enrolled = Integer.parseInt(tokens[9]);
				int class_cap = Integer.parseInt(tokens[10]);
				
				String[] buildings = building_lecture_room.split(" ");
				String building = buildings[0];

				
				String[] semester = semester_session.split(" ");
//				int year = Integer.parseInt(semester[1]);
				String year = semester[1];

				// if("Before 8:00AM".contains(lecture_time)){
				// return;
				// }

//				if (null == building) {
//					return;
//				}
//
//				if ("Unknown".contains(building)) {
//					return;
//				}
//
//				if ("UNKWN".contains(building)) {
//					return;
//				}
//
//				if ("Arr".contains(building)) {
//					return;
//				}

				String myKey = year.concat("-").concat(dept);

				intValue = new IntWritable(students_enrolled);
				word.set(myKey);

				context.write(word, intValue);
			} catch (NumberFormatException e) {
				System.out.println("NumberFormatException occured... Don't worry");
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
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

	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		Text word = new Text();
		Text word2 = new Text();

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\t");
			String key1 = fields[0].split("-")[0];
			String key2 = fields[0].split("-")[1] + "-" + fields[1];
//			System.out.println(key2);
			word.set(key1);
			word2.set(key2);
			context.write(word, word2);
		}
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();
		Text word = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String high_dept = "";
			int high_students = 0;
			
			for(Text tex : values){
				String dept = tex.toString().split("-")[0];
//				System.out.println(dept);
				int no_students = Integer.parseInt(tex.toString().split("-")[1]);
				if (no_students > high_students){
					high_dept = dept;
					high_students = no_students;
				}
			}
			result.set(high_students);
			String new_key = key.toString() + " -> " + high_dept;
			word.set(new_key);
			context.write(word, new Text(high_students+""));
			
		}
	}

	public static void main(String[] args) throws Exception {
		String temp = "Temp";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Get number of students per department per year");
		job.setJarByClass(Prob1.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Get depart with highest students per year");
		job2.setJarByClass(Prob1.class);
		job2.setMapperClass(Mapper2.class);
		// job2.setCombinerClass(FindIncreaseReducer.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path("Temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}