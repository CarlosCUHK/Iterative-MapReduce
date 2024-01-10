import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import javax.naming.Context;
import static java.lang.String.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PRPreProcess {
    public static enum ReachCounter {COUNT};

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] node2node = value.toString().split(" ");
            String src = node2node[0];
            String des = node2node[1];
            context.write(new Text(src), new Text(des));
            context.write(new Text(des), new Text("-1"));
        }

    }

    public static class MyReducer extends Reducer<Text, Text, Text, PRNodeWritable> {
            private HashSet<String> nodes = new HashSet<>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> uniqueDestinations = new HashSet<>();
            nodes.add(key.toString());
            for (Text value : values) {
                if (!value.toString().equals("-1")){
                     uniqueDestinations.add(value.toString());
                     nodes.add(value.toString());
                }
            }
            if (uniqueDestinations.size()> 0) {
            Text[] array = new Text[uniqueDestinations.size()];
            int index = 0;
            for (String destination : uniqueDestinations) {
                array[index++] = new Text(destination);
            }

            PRNodeWritable node = new PRNodeWritable(key, array);
            context.write(key, node);
            }
            else{

            context.write(key, new PRNodeWritable(key, new Text[0]));
            }
        }
        @Override
        public void cleanup(Context context){
            context.getCounter(PRPreProcess.ReachCounter.COUNT).setValue(nodes.size());
        }
    }
}