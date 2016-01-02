package com.textminer;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//int[] count;

//public TextReducer(){
//count = new int[5];
//for(int i=0;i<5;i++)count[i] = 0;
//}
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context output)
            throws IOException, InterruptedException {
	
      
    	//initialize the sum value with o
    	int Count = 0;
       
    	//iterate through the all the values with respect to key and sum up all of them
        for(IntWritable value: values){
        	Count+= value.get();

//if(key.equals("Perfect")||key.equals("good")||key.equals("Nice"))
  //           count[0]=count[0]+1;
        }
        // calculate the final count value
      Count=(Count/2);
      //pass the result to the output directory
      output.write(key, new IntWritable(Count));

//output.write(key,new IntWritable(count[0]));
    }
}
