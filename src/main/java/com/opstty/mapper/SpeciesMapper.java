/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.opstty.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author anas_
 */
public class SpeciesMapper extends Mapper {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String str = value.toString();
        String[] str_splitted =str.split(";");

        for(int index = 0; index < str_splitted.length; index++){
            if(index == 3 && !str_splitted[0].contains("GEOPOINT")) {
                context.write(new Text(str.split(";")[index]), one);
            }
        }
    }
}
