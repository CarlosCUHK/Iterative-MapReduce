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

public class PRAdjust {
    public static class PRAdjustMapper
            extends Mapper<Text, PRNodeWritable, Text, PRNodeWritable> {
        private Configuration conf;
        private int nodeNum;
        private double missingMASS;
        private double alpha;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            nodeNum = Integer.parseInt(conf.get("nodeNum"));
            missingMASS = Double.parseDouble(conf.get("missingMass"));
            alpha = Double.parseDouble(conf.get("alpha"));
        }

        public void map(Text key, PRNodeWritable value, Context context
        ) throws IOException, InterruptedException {
            double pagerank_value = alpha*(1.0/nodeNum) + (1-alpha)*(missingMASS/nodeNum + value.getPRvalue().get());
            value.setPRvalue(pagerank_value);
            context.write(key, value);
        }
    }

    public static class PRAdjustReducer
            extends Reducer<Text, PRNodeWritable, Text, PRNodeWritable> {
        public void reduce(Text key, Iterable<PRNodeWritable> values, Context context
        ) throws IOException, InterruptedException {
           for (PRNodeWritable value : values){
                context.write(key, value);
           }
        }
    }
}