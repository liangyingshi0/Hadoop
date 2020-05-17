package Work_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, MinMaxWritable>{
	//自定义分区，2017年一个区，2018年一个区
	@Override
	public int getPartition(Text key, MinMaxWritable value, int numPartitions) {
		String year = key.toString().split("-")[0];
		if(year.equals("2017")){
			return 0;
		}
		else
			return 1;
	}

}
