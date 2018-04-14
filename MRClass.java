//Importing Java Packages
import java.io.IOException;
import java.util.ArrayList;

//Importing Hadoop Packages
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

//Main Class that calls both the Mapper and Reducer Functions
public class MRClass extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "MRClass");
		job.setJarByClass(getClass());

		// Configuring the inputs and outputs
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		// Configuring the Mapper and Reducer Methods
		job.setMapperClass(Mapper_MissingCards.class);
		job.setReducerClass(Reducer_MissingCards.class);

		// Configuring Outputs
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitStatus = ToolRunner.run(new MRClass(), args);
		System.exit(exitStatus);
	}
}

class Mapper_MissingCards extends Mapper<LongWritable, Text, Text, IntWritable>
{
	Text _key = new Text();
	Text _value = new Text();

	public void map(LongWritable keys, Text values, Context context) throws IOException, InterruptedException
	{
		String _lines = values.toString();
		String[] _fields = _lines.split("\t");

		_key.set(_fields[0]);
		_value.set(_fields[1]);

		context.write(_key,new IntWritable(Integer.parseInt(_fields[1])));
	}
}

class Reducer_MissingCards extends Reducer<Text, IntWritable, Text, Text>
{
	Text missingCardsText = new Text();

	public void reduce(Text token, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {

		ArrayList<Integer> cardNumbers = new ArrayList<Integer>();

		int sm = 0;
		int tValue = 0;

		for (IntWritable val : counts) {
			sm += val.get();
			tValue = val.get();
			cardNumbers.add(tValue);
		}

		StringBuilder _stringBuilderMissingCards = new StringBuilder();

		if(sm<91){
			int j=1;
			while (j<=13){
				if(!cardNumbers.contains(j))
					_stringBuilderMissingCards.append(j).append(",");
				j++;
			}
			missingCardsText.set(_stringBuilderMissingCards.substring(0,_stringBuilderMissingCards.length()-1));
		}
		else{
			missingCardsText.set("All cards are there and there are no missing cards!!!!!!");
		}
		context.write(token, missingCardsText);
	}
}
