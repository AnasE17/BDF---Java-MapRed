/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.opstty.mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author anas_
 */
public class SpeciesMapper extends Mapper<Object, Text, Text, IntWritable> {
   private final static IntWritable one = new IntWritable(1);
    private Text text = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String s = value.toString();
        String[] splitted =s.split(";");

        for(int index = 0; index < splitted.length; index++){
            if(index == 3 && !splitted[0].contains("GEOPOINT")) {
                context.write(new Text(s.split(";")[index]), one);
            }
        }
    }
}
