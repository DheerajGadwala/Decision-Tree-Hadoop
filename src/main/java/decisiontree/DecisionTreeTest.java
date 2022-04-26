//package decisiontree;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//
//public class DecisionTreeTest extends Configured implements Tool {
//
//  private static final Logger logger = LogManager.getLogger(DecisionTreeTest.class); // log
//
//  public static class ReadSplits extends Mapper<Object, Text, Record, NullWritable> {
//
//  }
//
//
//
//  @Override
//  public int run(final String[] args) throws Exception {
//    String splitsFolder = args[0];
//    final Configuration conf = super.getConf();
//    final Job job = Job.getInstance(conf, "DecisionTreeTest");
//    job.setJarByClass(DecisionTreeTest.class);
//    final Configuration jobConf = job.getConfiguration();
//    jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
//
//    // Set up job's configuration.
//    job.setMapperClass(ReadSplits.class);
//    job.setMapOutputKeyClass(Record.class);
//    job.setMapOutputValueClass(NullWritable.class);
////    job.setOutputKeyClass(Text.class);
////    job.setOutputValueClass(Text.class);
//    job.setNumReduceTasks(0);
//    MultipleInputs.addInputPath(job, new Path(splitsFolder), TextInputFormat.class,
//            DecisionTreeTest.ReadSplits.class);
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//    try {
//      for (int i = 0; i < )
//    }
//
//  }
//
//  public static void main(final String[] args) {
//    if (args.length != 5) {
//      throw new Error("Five arguments required");
//    }
//    try {
//      ToolRunner.run(new DecisionTreeTest(), args);
//    } catch (final Exception e) {
//      logger.error("", e);
//    }
//  }
//
//}