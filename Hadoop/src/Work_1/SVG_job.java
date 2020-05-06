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
			w.setAverage(Integer.parseInt(value.toString()));
			context.write(new IntWritable(1), w);
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
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[]{"Work_1/svg_input","Work_1/svg_output"};
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
