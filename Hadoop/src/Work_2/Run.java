package Work_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import Work_1.SVG_job;

//运行类
public class Run {

	public static void main(String[] args) throws IOException {
		args = new String[]{"src/Work_2/input","src/Work_2/output"};
		Configuration conf = new Configuration();//配环境
		Job job = Job.getInstance(conf);//实例化
		job.setJarByClass(MinMaxValueDemo.class);//设运行Jar类型
	}

}
