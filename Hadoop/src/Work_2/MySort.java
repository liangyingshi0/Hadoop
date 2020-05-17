package Work_2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class MySort extends WritableComparator{
	//自定义排序
	public MySort() {
		super(Text.class, true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		//提取月份比较
		IntWritable V1 = new IntWritable(Integer.parseInt(a.toString().split("-")[1]));
		IntWritable V2 = new IntWritable(Integer.parseInt(b.toString().split("-")[1]));
		return V2.compareTo(V1);
	}

}
