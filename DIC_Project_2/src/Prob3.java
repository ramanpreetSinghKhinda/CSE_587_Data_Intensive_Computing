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

public class Prob3 {

	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private static IntWritable intValue;
		private Text word = new Text();
		private Text word1 = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] tokens = value.toString().split(",");
				if (tokens.length != 11) {
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
				
				if(building_lecture_room.contains("Arr") || buildings.length !=2 ){
					return;
				}
				
//				if()
				
				if(class_cap == 0){
					return;
				}
				
				if(class_cap < students_enrolled){
					students_enrolled = class_cap;
				}


				String myKey = building_lecture_room;

				intValue = new IntWritable(students_enrolled);
				word.set(myKey);
				word1.set((students_enrolled+"").concat("-").concat(class_cap+""));

				context.write(word, word1);
			} catch (NumberFormatException e) {
				System.out.println("NumberFormatException occured... Don't worry");
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
//		private IntWritable result = new IntWritable();
		private Text word = new Text();
		private Text word2 = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int tot_enrolled = 0;
			int tot_cap = 0;
			for (Text te : values){
//				System.out.println(te.toString());
				if (!te.toString().split("-")[0].equals("") && !te.toString().split("-")[1].equals("")){
					tot_enrolled += Integer.parseInt(te.toString().split("-")[0]);
					tot_cap += Integer.parseInt(te.toString().split("-")[1]);
				}
				else
					return;
			}
			word.set(key);
			word2.set(tot_enrolled + "-" + tot_cap);
			context.write(word, word2);
		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		Text word = new Text();
		Text word2 = new Text();

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\t");
			String hall = fields[0].split(" ")[0];
			String room = fields[0].split(" ")[1];
//			System.out.println(key2);
			word.set(hall);
			word2.set(fields[1]);
			context.write(word, word2);
		}
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();
		Text word = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int tcap =0;
			int tenroll = 0;
//			
			for(Text tex : values){
				int enroll = Integer.parseInt(tex.toString().split("-")[0]);
				int cap = Integer.parseInt(tex.toString().split("-")[1]);
				tcap += cap;
				tenroll += enroll;
			}
//			System.out.println(tcap + "-" + tenroll);
			int abc = tcap-tenroll;
//			System.out.println(abc+"");
			int emp = abc * 100 / tcap;
			System.out.println(emp+"");
//			float emp_percent = emp * 100;
//			result.set(high_students);
//			String new_key = key.toString() + " -> " + high_dept;
//			word.set(new_key);
			context.write(key, new Text(emp + "% of this buildings' capacity has been not used over the years"));
			
			
		}
	}

	public static void main(String[] args) throws Exception {
		String temp = "Temp";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Get number of students per department per year");
		job.setJarByClass(Prob3.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Reducer1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(temp));
		job.waitForCompletion(true);
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Get depart with highest students per year");
		job2.setJarByClass(Prob3.class);
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