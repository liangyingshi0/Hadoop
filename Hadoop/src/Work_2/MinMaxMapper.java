package Work_2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxMapper extends Mapper<Object, Text, Text, MinMaxWritable>{
	private MinMaxWritable outTuple = new MinMaxWritable();
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strs = value.toString().split(" ");
		//获取日期
		String strDate = strs[0];
		if (strDate == null){
			return;
		}
		//存最大最小值
		outTuple.setMax(Integer.parseInt(strs[1]));
		outTuple.setMin(Integer.parseInt(strs[2]));
		//将结果写入context
		context.write(new Text(strDate), outTuple);
	}
	
}
