import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Stripe {

	public static class MapClass extends Mapper<Object, Text, Text, LineWritable>{
		
		private String line;
		private String[] items;
		private long accessCount;
		private String hostName;
		private String serverIP;
		private long uploadTraffic;
		private long downloadTraffic;
		private String spName;
		private String userId;
		
		private Text outkey =new Text(), outvalue = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			line = value.toString();
			items = line.split("\\t");
			
			if(items[0].equals("time")){
				return ;
			}
			accessCount = 1;
			userId = items[1];
			serverIP = items[2];
			hostName = items[3];
			spName = items[4];
			uploadTraffic = Long.parseLong(items[5]);
			downloadTraffic = Long.parseLong(items[6]);
			LineWritable lw = new LineWritable(accessCount, userId, serverIP, hostName, 
												spName, uploadTraffic, downloadTraffic);
			outkey.set(userId);
//			outvalue.set(spName);
			
			context.write(outkey, lw);
		}
	}
	
	public static class ReduceClass extends Reducer<Text, LineWritable, LineWritable, NullWritable>{
		public void reduce(Text key, Iterable<LineWritable> values, Context context) throws IOException, InterruptedException{
			Map<String, LineWritable> map = new HashMap<String, LineWritable>();
			String userId = key.toString();
			Text outkey = new Text();
			
			for(LineWritable t : values){//for-each 循环中不能修改变量t的值, 只能使用
				String spName = t.getSpName();
				if(map.containsKey(spName)){
					LineWritable lineWritable = map.remove(spName);
					
					long at = lineWritable.getAccessCount();
					long ut = lineWritable.getUploadTraffic()+t.getUploadTraffic();
					long dt = lineWritable.getDownloadTraffic()+t.getDownloadTraffic();
					at++;
					lineWritable.setAccessCount(at);
					lineWritable.setDownloadTraffic(dt);
					lineWritable.setUploadTraffic(ut);
					map.put(spName, lineWritable);
				}else {
//					t.setAccessCount(1);
					LineWritable lineWritable = new LineWritable();
					lineWritable.setAccessCount(1);
					lineWritable.setUserID(t.getUserID());
					lineWritable.setHostName(t.getHostName());
					lineWritable.setDownloadTraffic(t.getDownloadTraffic());
					lineWritable.setServerIP(t.getServerIP());
					lineWritable.setUploadTraffic(t.getUploadTraffic());
					lineWritable.setSpName(t.getSpName());
					map.put(spName, lineWritable);
				}
			}
			for(String s : map.keySet()){
				context.write(map.get(s), NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args){
		Configuration conf = new Configuration();
		
		try {
			
			String[] paths=new GenericOptionsParser(conf,args).getRemainingArgs();

		    if (paths.length != 2) {
		        System.err.println("Usage: wordcount <in> <out>");
		        System.exit(2);
		      }
		      File outputDir = new File(paths[1]);
		      if(outputDir.exists()){
		    	  String[] files = outputDir.list();
		    	  for(String f : files){
		    		  File newfile = new File(outputDir+"/"+f);
		    		  newfile.delete();
		    	  }
		      	outputDir.delete();
		      	System.out.println("output path exists , now we deleted it");
		      }
		      
		      Job job = Job.getInstance(conf, "stripe");
		      
		      job.setJarByClass(Stripe.class);
		      
		      job.setMapperClass(MapClass.class);
		      job.setMapOutputKeyClass(Text.class);
		      job.setMapOutputValueClass(LineWritable.class);
		      
		      job.setReducerClass(ReduceClass.class);
//		      job.setNumReduceTasks(1);
		      
		      job.setOutputKeyClass(LineWritable.class);
		      job.setOutputValueClass(NullWritable.class);
		      
	          FileInputFormat.addInputPath(job, new Path(paths[0]));  
	          FileOutputFormat.setOutputPath(job, new Path(paths[1]));  
	          System.exit(job.waitForCompletion(true) ? 0:1);
		      
			
		} catch (IOException e) {
			e.printStackTrace();
		}catch (ClassNotFoundException e) {  
            e.printStackTrace();  
        } catch (InterruptedException e) {  
            e.printStackTrace();  
        } 
	}
}
