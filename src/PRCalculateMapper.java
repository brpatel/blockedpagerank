import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PRCalculateMapper  extends Mapper<Object, Text, Text, DoubleWritable>   {
	
	
	@Override
	protected void map(Object key, Text value, Context context) 
			throws IOException ,InterruptedException
	{
		String strippedValue = value.toString().split(PageRankConstants.STEP1_ID)[1];
		String[] values = strippedValue.toString().split(PageRankConstants.VALUE_SEPERATOR);
		Double nodePageRank = Double.parseDouble(values[0]);
		String[] outgoingNodes = null;
		if(values.length > 1)
			outgoingNodes= values[1].split(PageRankConstants.LIST_SEPERATOR);
		else
		{
			System.out.println("*************ERROR: PRJoinReducerL "+value.toString());
			return;
		}
		
		int	nodeDegree = outgoingNodes.length;
		
		nodeDegree = nodeDegree != 0 ? nodeDegree : 1;
		
		for (String outgoingNode : outgoingNodes)
		{
			Double prByDegree = nodePageRank/nodeDegree;
			//System.out.println("PRCalc - Map - Key: "+ outgoingNode + " Value: " + prByDegree.toString());
			context.write(new Text(outgoingNode), new DoubleWritable(prByDegree));
		}
		
		
	}

}
