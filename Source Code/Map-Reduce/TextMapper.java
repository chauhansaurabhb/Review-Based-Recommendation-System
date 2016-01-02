package com.textminer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TextMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
int count=0;
    @Override
    public void map(Object key, Text value, Context output) throws IOException,
            InterruptedException {

        // use tokenizer to split the line into words 
      
    	String word = new String("");
    	 StringTokenizer itr = new StringTokenizer(value.toString());
    	 System.out.println(value.toString());
    	 // iterate through each word
         while (itr.hasMoreTokens()) {
           word = itr.nextToken();
           // comapare the ech word with our specific word to classify onto positive and negative class and than pass this only specified key value pair to the reducer class
           if(word.contentEquals("good")||word.contentEquals("perfect")||word.contentEquals("great")||word.contentEquals("love")||word.contentEquals("better")||word.contentEquals("awesome")||word.contentEquals("nice")||word.contentEquals("happy")||word.contentEquals("excellent"))
           {
       		
       		output.write(new Text("Cluster Positive"), one);
       	}
       	else if(word.contentEquals("bad")||word.contentEquals("ok")||word.contentEquals("problem")||word.contentEquals("returned")||word.contentEquals("disappointed")||word.contentEquals("fault")||word.contentEquals("not")||word.contentEquals("worst")){
       		
       		output.write(new Text("Cluster Negative"), one);
       	}
       	
       	
         }
        /*
         for(int i=0;i<words.length;i++)
        {
        	
        	if(words[i].contentEquals("Nice")||words[i].contentEquals("Good")||words[i].contentEquals("Perfect")){
        		
        		output.write(new Text("Cluster 1"), one);
        	}
        	else if(words[i].contentEquals("Bad")||words[i].contentEquals("Ok")){
        		
        		output.write(new Text("Cluster 2"), one);
        	}
        	
        	else if(words[i].contentEquals("Great")){
        		
        		output.write(new Text("Cluster 3"), one);
        	}
        	
        	else if(words[i].contentEquals("Awesome")){
	
	output.write(new Text("Cluster 4"), one);
}
        }
        */
        
       // if(words.equals("Perfect")||words.equals("good")||words.equals("Nice"))
         // count=count+1;
        //Only the first word is the candidate name
       // output.write(new Text(words[0]), one);
    }
}
