package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class NumberTreesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text text = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            if (value.toString().contains("GEOPOINT"))
                return;
            else {
                StringTokenizer i = new StringTokenizer(value.toString(), "\n");
                while (i.hasMoreTokens()) {
                    text.set(i.nextToken().split(";")[3]);
                    context.write(text, one);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}