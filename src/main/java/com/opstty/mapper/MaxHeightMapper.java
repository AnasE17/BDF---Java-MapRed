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
public class MaxHeightMapper extends Mapper<Object, Text, Text, IntWritable>{
     private final Text t = new Text();


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer i = new StringTokenizer(value.toString(), "\n");


            while (i.hasMoreTokens()){
                t.set(i.nextToken());
                if(t.toString().contains(";")) {
                    Text text = new Text(t.toString().split(";")[3]);
                    String num = t.toString().split(";")[6];
                    if(!num.equals("")){
                        if (!text.equals(new Text("ESPECE"))) {
                            context.write(text, new IntWritable((int)Double.parseDouble(num)));
                        }
                    }
                }
            }
        }
}
