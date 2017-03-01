package CS499A3_Test.Movies;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapMovies extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static IntWritable one;
    private Text word = new Text();
    
    /**
     * map function of Mapper parent class takes a line of text at a time
     * splits to tokens and passes to the context as word along with value as one
     */
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] array = line.split(",");
		int rating = Integer.parseInt(array[2]);
		one = new IntWritable(rating);
		StringTokenizer st = new StringTokenizer(line,",");
		
		while(st.hasMoreTokens()){
			word.set(st.nextToken());
			st.nextToken();
			context.write(word,one);
			st.nextToken();
		}
		
	}
}
