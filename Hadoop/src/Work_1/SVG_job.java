package Work_1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//MapReduce程序
public class SVG_job {

	public static class SVGMap extends Mapper<LongWritable, Text, IntWritable, SVG_Writable>{
		private SVG_Writable w = new SVG_Writable();
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			w.setCount(1);
			w.setAverage(Integer.parseInt(value.toString().split(",")[1]));
			//context.write(new IntWritable(1), w);
			//new IntWritable(1)是新建了这个类的一个对象，而数值道1这是参数。在Hadoop中它相当于java中Integer整型变量，为这个变量赋值为1.
			//输出<1,{w}>    结果：1 53 88     意思：53个数，平均值为88
			context.write(null, w);
		}
	}
	
	public static class SVGReduce extends Reducer<IntWritable, SVG_Writable, IntWritable, SVG_Writable>{
		private SVG_Writable result = new SVG_Writable();
		protected void reduce(IntWritable key, Iterable<SVG_Writable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int count = 0;
			for (SVG_Writable val : values){
				sum += val.getCount()*val.getAverage();
				count += val.getCount();
			}
			result.setCount(count);
			result.setAverage(sum/count);
			context.write(null, result);
			//key是1，在map里context.write(new IntWritable(1), w);赋值了
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[]{"src/Work_1/svg_input","src/Work_1/svg_output"};
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SVG_job.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SVG_Writable.class);
		
		job.setMapperClass(SVGMap.class);
		job.setCombinerClass(SVGReduce.class);
		job.setReducerClass(SVGReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
