package blockedPR;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BlockedPRInputMapper  extends Mapper<Object, Text, Text, Text>   {
	
	/***
	 * Input: <u,v> : u v R
	 * Output: <u;v>: <u;v>
	 * 
	 */
	
	@Override
	protected void map(Object key, Text value, Context context) 
			throws IOException ,InterruptedException
	{
		String line = value.toString().trim();
		String[] values = line.split("\\s+");
		
		Text outputKey = new Text(values[0]);
		Text outputValue = new Text(values[1]);
		System.out.println("outputkey ===" + outputKey);
		double randomFloatValue = Double.parseDouble(values[2]);
		
		// compute filter parameters for netid dnm53
		double fromNetID = 0.35;
		double rejectMin = 0.99 * fromNetID;
		double rejectLimit = rejectMin + 0.01;
		// assume 0.0 <= rejectMin < rejectLimit <= 1.0
		
		boolean rejectResult = (((randomFloatValue >= rejectMin) && (randomFloatValue < rejectLimit)) ? false : true );
	
		if (rejectResult == true)
		{
			//BlockedPRConstants.TOTAL_NODES++;
			context.write(outputKey, outputValue);
		}
		System.out.println("inside blocked pr input mapper");
	}
	
	

}
