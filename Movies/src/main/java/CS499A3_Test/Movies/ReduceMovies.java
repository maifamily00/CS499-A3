package CS499A3_Test.Movies;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce class which is executed after the map class and takes
 * key(word) and corresponding values, sums all the values and write the
 * word along with the corresponding total occurances in the output
 * 
 * @author Raman
 */
public class ReduceMovies extends Reducer<Text, IntWritable, Text, IntWritable>{

	/**
	 * Method which performs the reduce operation and sums 
	 * all the occurrences of the word before passing it to be stored in output
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
	
		int sum = 0;
		int count = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		while(valuesIt.hasNext()){
			sum = sum + valuesIt.next().get();
			count++;
		}
		int average = sum/count;
		
		context.write(key, new IntWritable(average));
	}
}