package blockedPR;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class BlockedPRCalculateReducer extends Reducer<Text, Text, Text, Text> {



	@Override
	protected void reduce(Text key, 
			java.lang.Iterable<Text> values, 
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context) 
					throws IOException ,InterruptedException 
					{

		HashMap<Integer, String> sourceToDestinationMap = new HashMap<Integer, String>();
		HashMap<Integer, Double> destinationWithSourcePRMap = new HashMap<Integer, Double>(); // Key is destination , value is a comma separate value, PR(s), PR(s+1)
		context.getCounter(BlockedPRConstants.PR_COUNTER.BLOCK_COUNT).increment(1);
		int numberOfNodesinBlock = 0;
		for(Text value : values)
		{
			//count = 0;
			//System.out.println("inside blocked pr calculate reduce");
			//System.out.println("first value ==" + value);
			numberOfNodesinBlock++;	
			String[] splitValues = value.toString().split(BlockedPRConstants.VALUE_SEPERATOR);
			Integer srcNode = Integer.parseInt(splitValues[0]);
			//System.out.println(" split[1] = " + splitValues[0] +"_" + splitValues[1] +"_"+ splitValues[2]);
			sourceToDestinationMap.put(srcNode, splitValues[1]+"_"+splitValues[2]);

			String[] listOfDest =  splitValues[2].split(BlockedPRConstants.LIST_SEPERATOR);
			for( int i = 0; i < listOfDest.length; i++)
			{
				if(splitValues.length != 4 + i)
					System.out.println(value.toString());
				else{
					if(destinationWithSourcePRMap.containsKey(Integer.parseInt(listOfDest[i])))
					{
						Double pr = destinationWithSourcePRMap.get(Integer.parseInt(listOfDest[i]));
						pr += Double.parseDouble(splitValues[3+i].split(BlockedPRConstants.LIST_SEPERATOR)[1]);
						destinationWithSourcePRMap.put(Integer.parseInt(listOfDest[i]), pr);
					}
					else
					{
						Double pr = Double.parseDouble(splitValues[3 + i].split(BlockedPRConstants.LIST_SEPERATOR)[1]);
						destinationWithSourcePRMap.put(Integer.parseInt(listOfDest[i]), pr);				
					}
				}
			}
		}
		double residual = 0.0;
		HashSet <Integer> dstNodeSet = new HashSet<Integer>();
		//int numberOfNodesinBlock = sourceToDestinationMap.size();
		for(int j=0;j<2;j++)
		{
			dstNodeSet.clear();
			for(Map.Entry entry : sourceToDestinationMap.entrySet())
			{
				double pageRank = 0.0;
				Integer srcKey = (Integer) entry.getKey();
				String destinationNodes = (String) entry.getValue();	
				destinationNodes = destinationNodes.split(BlockedPRConstants.VALUE_SEPERATOR)[1];//list of destination nodes, [0] - PR(S)
				String[] destNodes = destinationNodes.split(BlockedPRConstants.LIST_SEPERATOR);// each destination
				Integer degree = destNodes.length;
				//System.out.println("degree ==== " + degree);
				
				if (destinationWithSourcePRMap.containsKey(srcKey)){
					pageRank = ((1.0/numberOfNodesinBlock) * 0.15) + ( 0.85* destinationWithSourcePRMap.get(srcKey));
					//newPR = pageRank/degree;
					//System.out.println("contains ==== " + srcKey + " == value === " + destinationWithSourcePRMap.get(srcKey) + "===  numberOfNodesinBlock ===" + numberOfNodesinBlock);
				}
				else
				{
					pageRank = ((1.0/numberOfNodesinBlock) * 0.15);
					//System.out.println("pageRank ==== " + pageRank);
					//System.out.println("no contains ==== " + srcKey + "===  numberOfNodesinBlock ===" + numberOfNodesinBlock);
				}
				double prByDegree = pageRank/degree;
				//System.out.println("pageRank ==== " + pageRank);
				//System.out.println("prByDegree ==== " + prByDegree);
				double pr =0.0;

				for( int i = 0; i < degree; i++)
				{	
					if (dstNodeSet.contains(Integer.parseInt(destNodes[i])))
					{
						//System.out.println("in if ==== ");
						pr = destinationWithSourcePRMap.get(Integer.parseInt(destNodes[i]));
						pr += prByDegree;
						destinationWithSourcePRMap.put(Integer.parseInt(destNodes[i]), pr);
					}
					else
					{
						//System.out.println("in else ");
						pr = prByDegree;
						destinationWithSourcePRMap.put(Integer.parseInt(destNodes[i]), prByDegree);
						dstNodeSet.add(Integer.parseInt(destNodes[i]));
					}
				}
				if (j==1)
				{					
					StringBuilder mapperValue = new StringBuilder(BlockedPRConstants.STEP1_ID + srcKey + BlockedPRConstants.VALUE_SEPERATOR  + pageRank +
							BlockedPRConstants.VALUE_SEPERATOR + sourceToDestinationMap.get(srcKey).split(BlockedPRConstants.VALUE_SEPERATOR)[1]);
					
					double oldPageRank = Double.parseDouble(sourceToDestinationMap.get(srcKey).split(BlockedPRConstants.VALUE_SEPERATOR)[0]);
					//System.out.println("pageRank ==== " + pageRank);
					residual += (Math.abs(oldPageRank - pageRank)/pageRank);
					context.getCounter(BlockedPRConstants.PR_COUNTER.NODES_COUNT).increment(1);
									
					context.write(key, new Text(mapperValue.toString()));
				}
			}

		}
		//System.out.println("residual ==== " + residual);
		context.getCounter(BlockedPRConstants.PR_COUNTER.RESIDUALS_SUM).increment((long)(residual/numberOfNodesinBlock)*10000);
	}
}
