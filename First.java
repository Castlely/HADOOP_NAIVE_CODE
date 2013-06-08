package org.apache.hadoop.examples;

import java.io.IOException;
import java.awt.*;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;






public class First {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}



























public class KMeans
{
   public int classes;
   public Vector cluster_centers;
   public int iterative_times;
   public Vector all_points;
   
   public KMeans(int c)
   {
      
      classes=c;
      cluster_centers=new Vector(c);
      all_points=new Vector();
      
      int i=0;
      for(i=0;i<c;i++)
      {cluster_centers.addElement(new Point(2,2));}
      
      
   }
   
   public void new_point_to_center()
   {
      int i=0;
      for(i=0;i<all_points.size();i++)  
      {
         all_points.get(i).class_no=get_closest_center_id(all_points.get(i).p);
      }
   }
   
   public void refresh_cluster_center()
   {
   
   }
   
   public void add_point(Point c)
   {
      AnlPoint a=new AnlPoint(c,1);
      all_points.addElement(a);
   }
   
   public Point get_cluster_center(int i)
   {
      return cluster_centers.get(i);
   }
   
   public int get_closest_center_id(Point x)
   {
      int i=0;
      int closest_id=0;
      int temp=0;
      int compare=0;
      for(i=0;i<classes;i++)
      {
         compare=distance(x,cluster_centers(i));
         if(compare>temp)
         {
            temp=compare; closest_id=i;
         }
      }
      return closest_id;
   }
   
   public double distance(Point x, Point y)
   {
      double m=x*x+y*y;
      return m;
   }
   
   public int clusterize()
   {
      iterative_times=0;
      
      
      
      return iterative_times;
   }
   
   public double KahanSum(double* input, int count)
   {
      double sum=0.0;
      double compensation=0.0;
      int i=0;
      for(i=0;i<count;i++)
      {
         double y=input[i]-compensation;
         double t=sum+y;
         compensation=(t-sum)-y
         sum=t;
      }
      return sum;
   }
}


public class AnlPoint
{
   public Point p;
   public int class_no;
   public AnlPoint(int no, Point x)
   {
      class_no=no;
      p.x=x.x;
      p.y=x.y;
   }
}

