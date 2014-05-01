import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PRJoinReducer extends Reducer<Text, Text, Text, Text> {



	@Override
	protected void reduce(Text key, 
			java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context) 
					throws IOException ,InterruptedException 
					{

		Double nodePR = (1 - PageRankConstants.D)/PageRankConstants.TOTAL_NODES;
		Double oldPageRank = 0.01; // Initial value
		StringBuilder nodeOutNodesList = new StringBuilder();

		for (Text value : values) {
			String stringValue = value.toString();

			if(stringValue.contains(PageRankConstants.STEP1_ID))
			{
				// Get the list of nodes having incoming links from this node
				String strippedValue = stringValue.split(PageRankConstants.STEP1_ID)[1];
				String[] step1Values = strippedValue.split(PageRankConstants.VALUE_SEPERATOR);
				oldPageRank = Double.parseDouble(step1Values[0]);
				if(step1Values.length > 1)
					nodeOutNodesList.append(step1Values[1]);
				else
				{
					System.out.println("*************ERROR: PRJoinReducerL "+stringValue);
				}
			}else 
			{
				// Calculate Page Rank
				String strippedValue = stringValue.split(PageRankConstants.STEP2_ID)[1];
				String[] prByDegreeValues = strippedValue.split(PageRankConstants.LIST_SEPERATOR);
				for(String prByDegree : prByDegreeValues)
					nodePR += PageRankConstants.D * Double.parseDouble(prByDegree);

			}
		} 
		
		StringBuilder reducerValue = new StringBuilder(PageRankConstants.STEP1_ID
				+ nodePR.toString() + PageRankConstants.VALUE_SEPERATOR
				+nodeOutNodesList.toString());


		//System.out.println("PRJoin - Reduce - Key: "+key.toString() + " Value: " + reducerValue.toString());
		Long residual = (long) (Math.abs(oldPageRank - nodePR)/nodePR)*10000;
		context.getCounter(PageRankConstants.PR_COUNTER.NODES_COUNT).increment(1);
		context.getCounter(PageRankConstants.PR_COUNTER.RESIDUALS_SUM).increment(residual);

		context.write(key, new Text(reducerValue.toString()));
					}

}
