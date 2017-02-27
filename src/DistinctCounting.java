import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistinctCounting {

	public static class MapClass extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text anEntry = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] items  = line.split("\\s");
			String userId = items[2];
			String spName = items[5];
			
			anEntry.set(userId + " " + spName);
			
			context.write(anEntry, one);
			
		}
		
	}
	
	public static class MapClass2 extends Mapper<Object, Text, Text, IntWritable>{
		private static final IntWritable one = new IntWritable(1);
		private Text keySpName = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
//			String userId = line.split("\\s")[0];
			String spName = line.split("\\s")[1];
			
			keySpName.set(spName);
			
			context.write(keySpName, one);
		}
	}
	
	public static class ReduceClass2 extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : values){
				sum++;
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, NullWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf1 = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		if(otherArgs.length != 4){
//			throw new RuntimeException("Usage: wordcount <in> <out>");
		      System.err.println("Usage: wordcount <in> <out>");
		      System.exit(2);
		}
		
		Job job1 = Job.getInstance(conf1, "Distinct Counting step 1 ");
		
		job1.setJarByClass(DistinctCounting.class);
		
		job1.setMapperClass(MapClass.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		job1.setReducerClass(ReduceClass.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		
		Configuration conf2 = new Configuration();
		
		Job job2 = Job.getInstance();
		
		job2.setJarByClass(DistinctCounting2.class);
		job2.setMapperClass(MapClass2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		
		job2.setReducerClass(ReduceClass2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
		
		job1.submit();
		while(!job1.isComplete()){}
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
