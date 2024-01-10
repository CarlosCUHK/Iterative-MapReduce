import java.io.DataInput;
import java.io.DataOutput;
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

public class PRNodeWritable implements Writable{
    private Text nodeId;
    private DoubleWritable pagerank_value;
    private MapWritable adjList;
    private IntWritable is_node;

    public PRNodeWritable() {
        this.nodeId = new Text();
        this.adjList = new MapWritable();
        this.pagerank_value = new DoubleWritable();
        this.is_node = new IntWritable(0);
    }

    public PRNodeWritable(Text nodeId, Text[] adjList) {
        this.nodeId = nodeId;
        this.adjList = new MapWritable();
        for (int i = 0; i < adjList.length; i++){
            this.adjList.put(new Text(Integer.toString(i)), adjList[i]);
        }
        this.pagerank_value = new DoubleWritable(0);
        this.is_node = new IntWritable(1);
    }

    public PRNodeWritable(Text nodeId, double pagerank_value){
        this.nodeId = nodeId;
        this.adjList = new MapWritable();
        this.pagerank_value = new DoubleWritable(pagerank_value);
        this.is_node = new IntWritable(0);
    }

    public IntWritable getFlag(){
        return this.is_node;
    }

    public Text getNodeId(){
        return this.nodeId;
    }

    public DoubleWritable getPRvalue(){
        return this.pagerank_value;
    }

    public MapWritable getAdjList(){
        return this.adjList;
    }

    public void setFlag(int flag){
        this.is_node.set(flag);
    }

    public void setNodeId(String nodeId){
        this.nodeId.set(nodeId);
    }

    public void setPRvalue(double pagerank_value){
        this.pagerank_value.set(pagerank_value);
    }

    public void setAdjList(Text[] adjList){
        this.adjList.clear();
        for (int i = 0; i < adjList.length; i++){
            this.adjList.put(new Text(Integer.toString(i)), adjList[i]);
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        nodeId.write(dataOutput);
        pagerank_value.write(dataOutput);
        adjList.write(dataOutput);
        is_node.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        nodeId.readFields(dataInput);
        pagerank_value.readFields(dataInput);
        adjList.readFields(dataInput);
        is_node.readFields(dataInput);
    }
}