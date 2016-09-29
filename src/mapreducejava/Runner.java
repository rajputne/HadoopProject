package mapreducejava;

import Business.Ip;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import static mapreducejava.IpReducer.ipList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Runner {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(Runner.class);
        conf.setJobName("ip-count");

        conf.setMapperClass(IpMapper.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setReducerClass(IpReducer.class);

        // take the input and output from the command line
        //String path = "/home/n/Downloads/Lecture2AllMaterial/HadoopLabs/Lab3.begin/Logs";
        //String outpath = "/home/n/Downloads/Lecture2AllMaterial/HadoopLabs/Lab3.begin/k";
        final File f = new File(Runner.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        String inFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/InputFiles/http_log.txt";
        String outFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/outputFiles/Results";

        FileInputFormat.setInputPaths(conf, new Path(inFiles));
        FileOutputFormat.setOutputPath(conf, new Path(outFiles));

        JobClient.runJob(conf);

        JobConf conf1 = new JobConf(Runner.class);
        conf1.setJobName("ip-count");

        conf1.setMapperClass(CountMapper.class);

        conf1.setMapOutputKeyClass(IntWritable.class);
        conf1.setMapOutputValueClass(Text.class);

        conf1.setReducerClass(CountReducer.class);

        FileInputFormat.setInputPaths(conf1, new Path(outFiles));
        //String outSortedFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/outputFiles2/Results2";
        //FileOutputFormat.setOutputPath(conf1, new Path(outSortedFiles));

        //JobClient.runJob(conf1);
        BufferedWriter writer = null;
        try {
            //create a temporary file
            String timeLog = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
            File logFile = new File(timeLog);

            // This will output the full path where the file will be written to...
            System.out.println(logFile.getCanonicalPath());

            writer = new BufferedWriter(new FileWriter(logFile));
            Iterator<Ip> itr = ipList.iterator();
            while (itr.hasNext()) {
                if (itr.next().getCount() > 100) {
                    writer.write(itr.next().getIp() + "-->" + itr.next().getCount() + "\n");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens...
                writer.close();
            } catch (Exception e) {
            }
        }

    }

}
