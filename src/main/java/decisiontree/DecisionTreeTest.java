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

/**
 * Test the decision tree.
 */
public class DecisionTreeTest extends Configured implements Tool {
  private static Node[] tree; // Trained decision tree [broadcast]

  // Counter to count the number of records.
  enum Counter {
    Record_Count,
    Accuracy
  }

  private static final Logger logger = LogManager.getLogger(DecisionTreeTest.class); // log

  // Represents each node in the tree.
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

  /**
   * Custom Comparator to ensure "dummy" nodes come before all.
   * Used as both Key Comparator and Group Comparator.
   */
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

  /**
   * Partition data such that all the nodes are sent to the same reduce task.
   */
  public static class CustomPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
      return 0; // set all the data to single reducer
    }
  }


  /**
   * 1. Broadcast the tree.
   * 2. At each map task, construct the tree in setup
   * 3. For each node, traverse the tree until a leaf node is reached by making a decision at each level.
   *    When leaf node is reached, if mean of current node == input Node's rating then prediction is correct. else wrong.
   * 4. emit (dummy, 1) for all predictions.
   * 5. emit (correct, 1) only when prediction is correct.
   */
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
        Record dataInput = new Record("0 " + data); // since the test data has no node id, append a dummy id (0) to each row and create a record.

        // Traverse to the bottom of the tree.
        Queue<Node> queue = new LinkedList<>();
        queue.add(tree[0]);

        // until leaf node
        while (!queue.isEmpty()) {
          Node curNode = queue.poll();
          int curNodeId = curNode.nodeId - 1;

          // Get left and right node from the tree.
          Node leftNode = (curNodeId * 2 + 1) < tree.length ? tree[curNodeId * 2 + 1] : null;
          Node rightNode = (curNodeId * 2 + 2) < tree.length ? tree[curNodeId * 2 + 2] : null;

          // no children so we have reached leaf node
          if (leftNode == null && rightNode == null) {
            // check if the prediction is correct [true].
            boolean res = Math.round(curNode.mean) == dataInput.getRating().get();
            context.write(new Text("dummy"), new IntWritable(1)); // keep the total count of the record
            if (res) {
              context.write(new Text("correct"), new IntWritable(1)); // prediction is correct
            }
          }

          // check if the given feature is less than split point.
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

  /**
   * Custom combiner.
   */
  public static class CustomCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
      int count = 0;
      if (key.toString().equals("dummy")) {
        for (IntWritable val : values) {
          count += val.get();
        }
      }
      else {
        for (IntWritable val : values) {
          count += val.get();
        }
      }
      context.write(key, new IntWritable(count));
    }
  }

  /**
   * Reducer uses order inversion technique to compute the Accuracy.
   * Accuracy = number of correct predictions / total number of values.
   * We get the total number of values using "dummy" nodes.
   */
  public static class EvaluateAccuracy extends Reducer<Text, IntWritable, NullWritable, NullWritable> {
    private double totalPredictions = 0;

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
        double res = count/totalPredictions * 100;
        context.getCounter(Counter.Accuracy).setValue((long) res);

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

    // Params
    String testInput = args[1];
    String testSample = args[3];
    String broadcastSplits = args[7];
    double sampleSize = Double.parseDouble(args[11]);

    // 1. Sample the test data set.
    DecisionTree.sampleJob(getConf(), testInput, testSample, sampleSize);

    // Configuration
    final Configuration conf = super.getConf();

    // instance of a job to broadcast tree.
    final Job job = Job.getInstance(conf, "DecisionTreeTest");
    job.setJarByClass(DecisionTreeTest.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

    // Set up job's configuration.
    MultipleInputs.addInputPath(job, new Path(testSample), TextInputFormat.class, DecisionTreeTest.ReadSplits.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setCombinerClass(CustomCombiner.class);
    job.setSortComparatorClass(CustomComp.class);
    job.setGroupingComparatorClass(CustomComp.class); // Send dummy nodes first.
    job.setPartitionerClass(CustomPartitioner.class); // Send all the records to single reduce task
    job.setReducerClass(EvaluateAccuracy.class); // Calculate Accuracy

    job.setNumReduceTasks(1);

    FileOutputFormat.setOutputPath(job, new Path("output"));
    job.addCacheFile(new Path(broadcastSplits + "/data").toUri());

    int res = job.waitForCompletion(true) ? 1 : 0;

    long count = job.getCounters().findCounter(Counter.Record_Count).getValue();
    logger.info("*****************************");
    logger.info(count);
    logger.info("*****************************");

    return res;
  }
}