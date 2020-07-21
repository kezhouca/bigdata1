import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {

	public static class StringSplitMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text time = new Text();
		private IntWritable count = new IntWritable();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
				String[] parts = value.toString().split(" ");
				String hour=parts[0];
				String language=parts[1];
				String title=parts[2];
				Integer viewCount=Integer.valueOf(parts[3]);
				if (language.equals("en") && !title.equals("Main_Page") && !title.startsWith("Special:")) {
					time.set(hour);
					count.set(viewCount);
					context.write(time,count);
				}
		}
	}

	public static class MaxReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int max = 0;
			for (IntWritable val : values) {
				if (val.get()>max)
					max = val.get();
			}
			result.set(max);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "page count");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(StringSplitMapper.class);
		job.setCombinerClass(MaxReducer.class);
		job.setReducerClass(MaxReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
