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
public class SortMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text text = new Text();


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer i = new StringTokenizer(value.toString(), "\n");
        while (i.hasMoreTokens()){
            text.set(i.nextToken());
            if(text.toString().contains(";")) {
                Text arrNum = new Text(text.toString().split(";")[3]); 
                String valNum = text.toString().split(";")[6]; 
                if(!valNum.equals("")){ 
                    if (!arrNum.equals(new Text("ESPECE"))) {
                        context.write(arrNum, new IntWritable((int)Double.parseDouble(valNum)));
                    }
                }
            }
        }
    }
}
