package mapreducejava;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Counts all of the hits for an ip. Outputs all ip's
 */
public class CountReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable k2, Iterator<Text> itrtr, OutputCollector<IntWritable, Text> oc, Reporter rprtr) throws IOException {

        Text text = new Text();
        // loop over the count and tally it up
        while (itrtr.hasNext()) {
            text = itrtr.next();
        }

        oc.collect(k2, text); //To change body of generated methods, choose Tools | Templates.
    }

}
