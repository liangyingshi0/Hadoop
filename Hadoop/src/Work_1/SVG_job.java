package Work_1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//MapReduce程序
public class SVG_job {
	
	//Mapper
	public static class SVGMap extends Mapper<LongWritable, Text, IntWritable, SVG_Writable>{
		private SVG_Writable w = new SVG_Writable();
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			w.setCount(1);
			w.setAverage(Integer.parseInt(value.toString().split(",")[1]));//CSV文件是以逗号隔开的，该行表示取出分数的值
			context.write(new IntWritable(1), w);
			//new IntWritable(1)是新建了这个类的一个对象，而数值道1这是参数。在Hadoop中它相当于java中Integer整型变量，为这个变量赋值为1.
			//输出<1,{w}>    若最终在reducer输出这个形式，则结果：1 53 88     意思：53个数，平均值为88
		}
	}
	
	//Combiner
	public static class SVGCombine extends Reducer<IntWritable, SVG_Writable, IntWritable, SVG_Writable>{
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
			//key是1，在map里context.write(new IntWritable(1), w);赋值
		}
	}
	
	//Reducer
	//Combiner虽然与Reducer的内容相似，但它们的输入输出格式不一样，所以分开两个写。
	//因为Reduce输出是最终输出文本的内容，它只需要一个平均值，所以可以最终结果可以不需要用SVG_Writable的形式
	public static class SVGReduce extends Reducer<IntWritable, SVG_Writable, NullWritable, IntWritable>{
		//private SVG_Writable result = new SVG_Writable();
		protected void reduce(IntWritable key, Iterable<SVG_Writable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int count = 0;
			for (SVG_Writable val : values){
				sum += val.getCount()*val.getAverage();
				count += val.getCount();
			}
			//result.setCount(count);
			//result.setAverage(sum/count);
			context.write(NullWritable.get(), new IntWritable(sum/count));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//运行前，要先把svg_output文件夹删去，否则会报错；两个数据文件要放在svg_input文件夹里
		args = new String[]{"src/Work_1/svg_input","src/Work_1/svg_output"};
		Configuration conf = new Configuration();//配环境
		Job job = Job.getInstance(conf);//实例化
		job.setJarByClass(SVG_job.class);//设运行Jar类型
		
		//该两行设置输出类型是会影响MapReduce的输出：Map,Reducer
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		//更改Map的输出类型
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(SVG_Writable.class);
		
		job.setMapperClass(SVGMap.class);
		job.setCombinerClass(SVGCombine.class);
		job.setReducerClass(SVGReduce.class);
		
		//设文件输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
