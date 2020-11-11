/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.opstty.reducer;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author anas_
 */
public class MaxHeightReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
     private final IntWritable result = new IntWritable();

    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int max = 0;
         for (Iterator<IntWritable> it = values.iterator(); it.hasNext();) {
             IntWritable v = it.next();
             int i = v.get();
             if (i > max)
             {
                 max = i;
             }
         }
        result.set(max);
        context.write(text, result);
    }
}
