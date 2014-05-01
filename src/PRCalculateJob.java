import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PRCalculateJob {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		 
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(PRCalculateJob.class);
		 
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(DoubleWritable.class);
		 
		  job.setMapperClass(PRCalculateMapper.class);
		  job.setReducerClass(PRCalculateReducer.class);
		 
		  job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		 
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  
		 
		  job.waitForCompletion(true);

	}

}
