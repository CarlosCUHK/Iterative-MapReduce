import java.io.IOException;
import java.util.Map;


import javax.naming.Context;
import javax.naming.Context;
import java.util.LinkedList;

import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import static java.lang.String.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {
    public static enum ReachCounter {MISSING_MASS};
    public static class PageRankMapper
            extends Mapper<Text, PRNodeWritable, Text, PRNodeWritable> {
        private Configuration conf;

        private int nodeNum;
        private boolean first_iter;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            nodeNum = Integer.parseInt(conf.get("nodeNum"));
            first_iter = conf.get("first_iter").equals("true");

        }

        public void map(Text key, PRNodeWritable value, Context context
        ) throws IOException, InterruptedException {
            double distributed_pr = 0;
            MapWritable myMap = value.getAdjList();
            MapWritable adjList = value.getAdjList();
            if (first_iter){
                value.setPRvalue(1.0/nodeNum);
            }
            if (adjList.size() > 0){
                distributed_pr = value.getPRvalue().get()/adjList.size();
            }

            context.write(key, value);
            for (Writable neighbour: adjList.values()){
                context.write((Text)neighbour, new PRNodeWritable((Text)neighbour, distributed_pr));
            }

        }

    }

    public static class PageRankReducer
            extends Reducer<Text, PRNodeWritable, Text, PRNodeWritable> {

        private double mass = 0;
        public void reduce(Text key, Iterable<PRNodeWritable> values, Context context
        ) throws IOException, InterruptedException {
            PRNodeWritable node = new PRNodeWritable();
            double pagerank_sum = 0;
            for (PRNodeWritable value : values){
                if (value.getFlag().get() == 1){
                    node.setNodeId(value.getNodeId().toString());
                    node.setFlag(value.getFlag().get());
                    MapWritable tmp = value.getAdjList();
                    Text[] my_tmp = new Text[tmp.size()];
                    int index = 0;
                    for (Writable current : tmp.values()){
                        my_tmp[index++] = (Text)current;
                    }
                    node.setAdjList(my_tmp);
                }
                else{
                    pagerank_sum += value.getPRvalue().get();
                }
            }
            mass += pagerank_sum;
            MapWritable tmp = node.getAdjList();
            node.setPRvalue(pagerank_sum);
            context.write(key, node);
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            double missMass = 1 - mass;
            Long longMissMass = new Long((long) Float.floatToIntBits((float)missMass));
            context.getCounter(ReachCounter.MISSING_MASS).setValue(longMissMass);
        }
    }

    public static class OutputMapper
            extends Mapper<Text, PRNodeWritable, Text, DoubleWritable> {
        private Configuration conf;
        private double threshold;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            threshold = Double.parseDouble(conf.get("threshold"));
        }

        public void map(Text key, PRNodeWritable value, Context context
        ) throws IOException, InterruptedException {
            if (value.getPRvalue().get() > threshold){
                context.write(key, value.getPRvalue());
            }
        }
    }

    public static class OutputReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private Configuration conf;
        private double threshold;

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context
        ) throws IOException, InterruptedException {
            for (DoubleWritable value : values){
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        double alpha = Double.parseDouble(args[0]);
        int iteration = Integer.parseInt(args[1]);
        double threshold = Double.parseDouble(args[2]);
        String infile = args[3];
        String outdir = args[4];
        Configuration conf = new Configuration();
        String workplace = "intermediate/";
        Job job = Job.getInstance(conf, "preprocess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PRPreProcess.MyMapper.class);
        job.setReducerClass(PRPreProcess.MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PRNodeWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(infile));
        FileOutputFormat.setOutputPath(job, new Path(workplace + Integer.toString(0) + "jobB"));

        System.out.println(job.waitForCompletion(true) ? "success" : "fail");
        long nodeNum = job.getCounters().findCounter(PRPreProcess.ReachCounter.COUNT).getValue();
        conf.set("alpha",  String.valueOf((alpha)));
        conf.set("nodeNum", String.valueOf(nodeNum));
        conf.set("first_iter", "true");
System.out.println("=========================");
System.out.println("=============nodeNum=========");
System.out.println(nodeNum);
        for (int i = 1; i <= iteration; i++) {
            Job jobA = Job.getInstance(conf, "PageRank" + Integer.toString(i));
            jobA.setJarByClass(PageRank.class);
            jobA.setMapperClass(PageRank.PageRankMapper.class);
            jobA.setReducerClass(PageRank.PageRankReducer.class);
            jobA.setOutputKeyClass(Text.class);
            jobA.setOutputValueClass(PRNodeWritable.class);
            jobA.setInputFormatClass(SequenceFileInputFormat.class);
            jobA.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(jobA, new Path(workplace + Integer.toString(i - 1) + "jobB"));
            FileOutputFormat.setOutputPath(jobA, new Path(workplace + Integer.toString(i) + "jobA"));
            System.out.println(jobA.waitForCompletion(true) ? "success" : "fail");
            float missingMass = Float.intBitsToFloat((int)jobA.getCounters().findCounter(ReachCounter.MISSING_MASS).getValue());
            conf.set("missingMass", String.valueOf(missingMass));
            System.out.println("=========================");
            System.out.println("=========================");
            System.out.println("=========missingmass================");
            System.out.println(missingMass);
            Job jobB = Job.getInstance(conf, "PRAdjust" + Integer.toString(i));
            jobB.setJarByClass(PRAdjust.class);
            jobB.setMapperClass(PRAdjust.PRAdjustMapper.class);
            jobB.setReducerClass(PRAdjust.PRAdjustReducer.class);
            jobB.setOutputKeyClass(Text.class);
            jobB.setOutputValueClass(PRNodeWritable.class);
            jobB.setInputFormatClass(SequenceFileInputFormat.class);
            jobB.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.addInputPath(jobB, new Path(workplace + Integer.toString(i) + "jobA"));
            FileOutputFormat.setOutputPath(jobB, new Path(workplace + Integer.toString(i) + "jobB"));
            System.out.println(jobB.waitForCompletion(true) ? "success" : "fail");
            conf.set("first_iter", "false");
        }
        conf.set("threshold", String.valueOf(threshold));
        Job outputjob = Job.getInstance(conf, "postprocess");
        outputjob.setJarByClass(PageRank.class);
        outputjob.setMapperClass(PageRank.OutputMapper.class);
        outputjob.setReducerClass(PageRank.OutputReducer.class);
        outputjob.setOutputKeyClass(Text.class);
        outputjob.setOutputValueClass(DoubleWritable.class);
        outputjob.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(outputjob, new Path(workplace + Integer.toString(iteration) + "jobB"));
        FileOutputFormat.setOutputPath(outputjob, new Path(outdir));
        System.out.println(outputjob.waitForCompletion(true) ? "success" : "fail");
    }
}