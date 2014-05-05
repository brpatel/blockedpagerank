package blockedPR;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BlockedPROutputReducer extends Reducer<Text, Text, Integer, Text> {



	@Override
	protected void reduce(Text key, 
			java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Integer, Text>.Context context) 
					throws IOException ,InterruptedException 
					{
		Integer blockID = Integer.parseInt(key.toString());
		
		int maxNode = -1;
		Double maxPR = 0.0;
		
		ArrayList<Integer> nodeList = new ArrayList <Integer>();
		ArrayList<Double> prList = new ArrayList <Double>();
		for (Text value : values) {
			String[] srcVal = value.toString().split(BlockedPRConstants.LIST_SEPERATOR);
			
			if (maxNode < Integer.parseInt(srcVal[0]))
			{
				maxNode = Integer.parseInt(srcVal[0]);
				maxPR = Double.parseDouble(srcVal[1]);
			}
		} 

		context.write(blockID, new Text(maxPR.toString()));
	}
}
