import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PRJoinJob {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		 
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(PRJoinJob.class);
		 
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		 
		  job.setMapperClass(PRJoinMapper.class);
		  job.setReducerClass(PRJoinReducer.class);
		 
		  job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		 
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileInputFormat.addInputPath(job, new Path(args[1]));
		  FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  
		 
		  job.waitForCompletion(true);

	}

}
