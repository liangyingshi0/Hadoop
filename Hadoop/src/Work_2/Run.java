package Work_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//运行类
public class Run {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[]{"src/Work_2/input","src/Work_2/output"};
		Configuration conf = new Configuration();//配环境
		Job job = Job.getInstance(conf);//实例化
		job.setJarByClass(Run.class);//设运行Jar类型
		job.setMapperClass(MinMaxMapper.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setSortComparatorClass(MySort.class);
		job.setCombinerClass(MinMaxReducer.class);
		job.setReducerClass(MinMaxReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxWritable.class);
		
		//设文件输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		job.waitForCompletion(true);
	}

}
