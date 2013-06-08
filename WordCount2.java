public class WordCount2
{
   public static void main(String[] args)
   {
      JobClient client=new JobClient();
      JobConf conf=new JobConf(WordCount2.class);
      FileInputFormat.addInputPath(conf,new Path(args[0]));
      FileOutputFormat.setOutputPath(conf,new Path(args[1]));
      
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(LongWritable.class);
      conf.setMapperClass(TokenCountMapper.class);
      conf.setCombinerClass(LongSumReducer.class);
      conf.setReducerClass(LongSumReducer.class);
      
      client.setConf(conf);
      try
      {
         JobClient.runJob(conf);
      }
      catch(Exception e)
      {e.printStackTrace();}
   }
}
