package blockedPR;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class BlockedPRJob {


	public static void main(String[] args) throws Exception{

		CreateBlockList();
		
		PRInputJob(BlockedPRConstants.INPUT_JOB_INPUT, BlockedPRConstants.INPUT_JOB_OUTPUT);
		CopyOutputToInput(BlockedPRConstants.INPUT_JOB_OUTPUT, BlockedPRConstants.CALCULATE_JOB_INPUT);

		for(int iteration =0; iteration < 2; iteration++){
			System.out.println("Iteration: " + iteration+1);
			PRCalculateJob(BlockedPRConstants.CALCULATE_JOB_INPUT, BlockedPRConstants.CALCULATE_JOB_OUTPUT);
			CopyOutputToInput(BlockedPRConstants.CALCULATE_JOB_OUTPUT, BlockedPRConstants.JOIN_JOB_INPUT);

			PRJoinJob(BlockedPRConstants.JOIN_JOB_INPUT, BlockedPRConstants.CALCULATE_JOB_INPUT, BlockedPRConstants.JOIN_JOB_OUTPUT);
			CopyOutputToInput(BlockedPRConstants.JOIN_JOB_OUTPUT, BlockedPRConstants.CALCULATE_JOB_INPUT);
		}
	}

	private static void CreateBlockList() throws IOException
	{
		Path pt=new Path(BlockedPRConstants.BLOCK_PATH_STRING);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;
		while ((line = br.readLine()) != null){
			BlockedPRConstants.blockList.add(Integer.parseInt(line));

		}

	}

	private static void CopyOutputToInput(String outputPathString,
			String inputPathString) throws IOException {

		Configuration conf = new Configuration();

		Path outputPath = new Path(outputPathString);
		Path inputPath = new Path(inputPathString);

		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(inputPath)) {
			fileSystem.delete(inputPath, true);
		}
		fileSystem.rename(outputPath, inputPath);


	}

	public static void  PRInputJob(String inputPathString, String outputPathString)
			throws Exception
			{
		Configuration conf = new Configuration();	
		Path outputPath = new Path(outputPathString);
		Path inputPath = new Path(inputPathString);

		FileSystem dfs = FileSystem.get(outputPath.toUri(), conf);
		if (dfs.exists(outputPath)) {
			dfs.delete(outputPath, true);
		}

		Job job = Job.getInstance(conf);
		job.setJarByClass(PRInputJob.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(BlockedPRInputMapper.class);
		job.setReducerClass(BlockedPRInputReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);


		job.waitForCompletion(true);
			}

	public static void  PRCalculateJob(String inputPathString, String outputPathString)
			throws Exception
			{
		Configuration conf = new Configuration();

		Path outputPath = new Path(outputPathString);
		Path inputPath = new Path(inputPathString);

		FileSystem dfs = FileSystem.get(outputPath.toUri(), conf);
		if (dfs.exists(outputPath)) {
			dfs.delete(outputPath, true);
		}


		Job job = Job.getInstance(conf);
		job.setJarByClass(PRCalculateJob.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(PRCalculateMapper.class);
		job.setReducerClass(PRCalculateReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);


		job.waitForCompletion(true);

			}

	public static void  PRJoinJob(String inputPathString, String secondaryInputPathSring, String outputPathString)
			throws Exception
			{
		Configuration conf = new Configuration();

		Path outputPath = new Path(outputPathString);
		Path inputPath = new Path(inputPathString);
		Path secondaryInputPath = new Path(secondaryInputPathSring);

		FileSystem dfs = FileSystem.get(outputPath.toUri(), conf);
		if (dfs.exists(outputPath)) {
			dfs.delete(outputPath, true);
		}


		Job job = Job.getInstance(conf);
		job.setJarByClass(PRJoinJob.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(PRJoinMapper.class);
		job.setReducerClass(PRJoinReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileInputFormat.addInputPath(job, secondaryInputPath);
		FileOutputFormat.setOutputPath(job, outputPath);


		job.waitForCompletion(true);

		// Print Residual average
		Long residualSum = job.getCounters().findCounter(BlockedPRConstants.PR_COUNTER.RESIDUALS_SUM).getValue();
		Long nodesCount = job.getCounters().findCounter(BlockedPRConstants.PR_COUNTER.NODES_COUNT).getValue();
		System.out.println("Residual Sum: "+residualSum.toString()+" Nodes: "+nodesCount.toString());
		nodesCount = nodesCount !=0 ? nodesCount : 1;
		Double residualAvg = (double) residualSum/nodesCount;
		System.out.println("Average Residual: "+ residualAvg.toString());

			}

}
