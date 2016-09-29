package mapreducejava;

import Business.Ip;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Counts all of the hits for an ip. Outputs all ip's
 */
public class IpReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    static TreeSet<Ip> ipList = new TreeSet<>();

    public void reduce(Text ip, Iterator<IntWritable> counts,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        int totalCount = 0;

        // loop over the count and tally it up
        while (counts.hasNext()) {
            IntWritable count = counts.next();
            totalCount += count.get();
        }
        Ip ipp = new Ip();
        Ip ipCount = new Ip();
        ipCount.setIp(ip.toString());
        ipCount.setCount(totalCount);
        ipList.add(ipCount);

        Iterator<Ip> itr = ipList.iterator();
        while (itr.hasNext()) {
            ipp = itr.next();
        }

        output.collect(new Text(ipp.getIp()), new IntWritable(ipp.getCount()));
    }

}
