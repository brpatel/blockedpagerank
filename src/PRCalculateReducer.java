import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PRCalculateReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	
	
	
	@Override
	protected void reduce(Text key, 
			java.lang.Iterable<DoubleWritable> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, DoubleWritable, Text, Text>.Context context) 
			throws IOException ,InterruptedException 
			{
				
				StringBuilder nodePRByDegList = new StringBuilder();
				for (DoubleWritable value : values) {
					nodePRByDegList.append(value.toString());
					nodePRByDegList.append(PageRankConstants.LIST_SEPERATOR);
				} 
				StringBuilder reducerValue = new StringBuilder(PageRankConstants.STEP2_ID
						+nodePRByDegList.substring(0, nodePRByDegList.length()-1));
				
				
				//System.out.println("PRCalc - Reduce - Key: "+key.toString() + " Value: " + reducerValue.toString());
				
				context.write(key, new Text(reducerValue.toString()));
			}

}
