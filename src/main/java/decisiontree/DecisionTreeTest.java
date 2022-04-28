package decisiontree;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

public class DecisionTreeTest extends Configured implements Tool {
  private static Node[] tree; // Trained decision tree [broadcast]


  String inputFolder, levelDataFolder, treeLevelFolder,
          splitsFolder, broadcastSplits, leafNodesFolder;

  double varianceCap;
  enum Counter {
    Record_Count
  }
  private static final Logger logger = LogManager.getLogger(DecisionTreeTest.class); // log

  public static class Node {

    int nodeId;
    int featureId;
    double splitPoint;
    double mean;

    public Node(String line) {
      String[] dataPoints = line.split(" ");
      this.nodeId = Integer.parseInt(dataPoints[0]);
      this.featureId = Integer.parseInt(dataPoints[1]);
      this.splitPoint = Double.parseDouble(dataPoints[2]);
      this.mean = Double.parseDouble(dataPoints[3]);
    }


    @Override
    public String toString() {

      StringBuilder sb = new StringBuilder();
      sb.append(nodeId)
              .append(" ")
              .append(featureId).append(" ")
              .append(splitPoint).append(" ");
      sb.append(mean);
      return sb.toString();
    }
  }

  public static class CustomComp extends WritableComparator {
    public CustomComp() {
      super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
      // Type cast it to text
      Text key1 = (Text) o1;
      Text key2 = (Text) o2;

      if (key1.compareTo(key2) == 0) {
        return 0;
      }
      else if (key1.toString().compareTo("dummy") == 0) {
        return -1;
      }
      else {
        return 1;
      }
    }
  }

  public static class CustomPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
      return 0; // set all the data to single reducer
    }
  }


  public static class ReadSplits extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void setup(final Context context) throws IOException {

      // 2. Distributed cache could have many file, which one do we need?
      Path[] cacheFiles = context.getLocalCacheFiles(); // paths to all the cached file.

      for (Path file : cacheFiles) {
        // try to read the file.
        try {

          // Create a FileSystem object and pass the configuration object in it. The FileSystem
          // is an abstract base class for a fairly generic filesystem.
          // All user code that may potentially use the Hadoop Distributed File System should
          // be written to use a FileSystem object.

          // FileSystem is an abstract class that is used a base for generic file system.
          FileInputStream fileSystem = new FileInputStream(new File(file.toUri().getPath()));

          // We open the file using FileSystem object, convert the input byte stream to character
          // streams using InputStreamReader and wrap it in BufferedReader to make it more efficient
          BufferedReader reader;

          reader = new BufferedReader(new InputStreamReader(fileSystem));
          String line;
          line = reader.readLine(); // get the number of layers
          int numOfNode = (int) Math.pow(2, Integer.parseInt(line)) - 1;
          tree = new Node[numOfNode];

          while ((line = reader.readLine()) != null) {
            Node node = new Node(line);
            tree[node.nodeId - 1] = node;
          }
        } catch (Exception e) {
          logger.info(e.getMessage());
          System.exit(1);
        }
      }
    }

    @Override
    public void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
      final StringTokenizer itr = new StringTokenizer(value.toString(), "\n", false);

      while (itr.hasMoreTokens()) {

        String data = itr.nextToken();

        context.getCounter(Counter.Record_Count).increment(1);
        Record dataInput = new Record("0 " + data);

        Queue<Node> queue = new LinkedList<>();

        queue.add(tree[0]);

        // until leaf node
        while (!queue.isEmpty()) {
          Node curNode = queue.poll();
          int curNodeId = curNode.nodeId - 1;

          Node leftNode = (curNodeId * 2 + 1) < tree.length ? tree[curNodeId * 2 + 1] : null;
          Node rightNode = (curNodeId * 2 + 2) < tree.length ? tree[curNodeId * 2 + 2] : null;

          // no children so we have reached leaf node
          if (leftNode == null && rightNode == null) {
            boolean res = Math.round(curNode.mean) == dataInput.getRating().get();
            context.write(new Text("dummy"), new IntWritable(1)); // keep the total count of the record
            if (res) {
              context.write(new Text("correct"), new IntWritable(1)); // prediction
            }
          }

          // check if the given feature is less h
          else if (dataInput.getFeature(curNode.featureId) < curNode.splitPoint) {
            // move left
            queue.add(leftNode);
          } else {
            // move right
            queue.add(rightNode);
          }
        }
      }
    }
  }

  public static class EvaluateAccuracy extends Reducer<Text, IntWritable, NullWritable, NullWritable> {
    double totalPredictions = 0;

    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) {

      if (key.toString().equals("dummy")) {
        for (IntWritable val : values) {
          totalPredictions += val.get();
        }
      }
      else {
        double count = 0;
        for (IntWritable val : values) {
          count += val.get();
        }
        logger.info("*******************************");
        logger.info("Total : " + totalPredictions);
        logger.info("count : " + count);
        logger.info(count/totalPredictions * 100);
        logger.info("*******************************");
      }
    }
  }

  @Override
  public int run(final String[] args) throws Exception {

    // Read Params
    inputFolder = args[0];
    levelDataFolder = args[1];
    treeLevelFolder = args[2];
    splitsFolder = args[3];
    varianceCap = Double.parseDouble(args[4]); // ensure that the variance of the split is > 0.08 to avoid training data that is very similar to each other.
    broadcastSplits = args[5];
    leafNodesFolder = args[6];

    // Configuration
    final Configuration conf = super.getConf();

    // instance of a job to broadcast tree.
    final Job job = Job.getInstance(conf, "DecisionTreeTest");
    job.setJarByClass(DecisionTreeTest.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

    // Set up job's configuration.
    job.setMapperClass(ReadSplits.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setSortComparatorClass(CustomComp.class);
    job.setGroupingComparatorClass(CustomComp.class);
    job.setPartitionerClass(CustomPartitioner.class);
    job.setReducerClass(EvaluateAccuracy.class);
//    job.setCombinerClass();
//    job.setOutputKeyClass(Record.class);
//    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);

    MultipleInputs.addInputPath(job, new Path(inputFolder), TextInputFormat.class,
            DecisionTreeTest.ReadSplits.class);
    FileOutputFormat.setOutputPath(job, new Path("output"));
    job.addCacheFile(new Path(broadcastSplits + "/data").toUri());

    job.waitForCompletion(true);
    long count = job.getCounters().findCounter(Counter.Record_Count).getValue();
    logger.info("*****************************");
    logger.info(count);
    logger.info("*****************************");
    return 1;
  }



}