import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount
{

    public static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable>
    {

        public void map(LongWritable key, Text value,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException
        {
            String line = value.toString();
            IntWritable one = new IntWritable(1);;
            String[] items = line.split("\\s");
            for(String word : items){
            	output.collect(new Text(word), one);
            }
        }
    }
    public static class Reduce extends MapReduceBase implements
            Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException
        {
            int sum = 0;
            while (values.hasNext()){
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception
    {
        /**
        * JobConf：map/reduce的job配置类，向hadoop框架描述map-reduce执行的工作
        * 构造方法：JobConf()、JobConf(Class exampleClass)、JobConf(Configuration conf)等
        */
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");          //设置一个用户定义的job名称
        conf.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        conf.setOutputValueClass(IntWritable.class);  //为job输出设置value类
        conf.setMapperClass(Map.class);        //为job设置Mapper类
        conf.setNumReduceTasks(1);
        conf.setCombinerClass(Reduce.class);      //为job设置Combiner类
        conf.setReducerClass(Reduce.class);        //为job设置Reduce类
        conf.setInputFormat(TextInputFormat.class);    //为map-reduce任务设置InputFormat实现类
        conf.setOutputFormat(TextOutputFormat.class);  //为map-reduce任务设置OutputFormat实现类

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        long startTime = System.currentTimeMillis();
        JobClient.runJob(conf);        //运行一个job
        System.out.println("job completed in " + (System.currentTimeMillis() - startTime) + " ms");
    }
}