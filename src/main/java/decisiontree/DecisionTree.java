package decisiontree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DecisionTree extends Configured implements Tool {

  enum Counters {
    NodeCount
  }

  String trainInput, levelData, treeLevel, splits,
      broadcastSplits, leafNodes, trainSample;

  int maxDepth;
  double varianceCap, sampleSize;

  private static final Logger logger = LogManager.getLogger(DecisionTree.class); // log

  // Mapper: Split the data based on each split point for every feature.
  //              1. read the data and create an object using class Record.
  //              2. define split points for the features [it is equally spaced values]
  //              3. for each feature of the given record -> split it by each split point.
  //                 e.g. If feature 1 = 0.5 then for split point = 0.2 and 0.4 the node is right (split 1);
  //                 SP = 0.6 and 0.8 the node is left (split 0).
  //              4. To calculate mean; for each record emit a dummy value for every split point. Order Inversion Technique.
  public static class SplitMapper extends Mapper<Object, Text, DTKey, DTValue> {

    // equal spaced split points to split the data
    double[] splitPoints = new double[] {0.2, 0.4, 0.6, 0.8};

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

      final StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

      while (itr.hasMoreTokens()) {
        Record r = new Record(itr.nextToken()); // store the record in an object.
        int nodeId = r.getNodeId(); // get the node id of the record

        // Iterate through all the columns (features) of the record
        for (int featureId = 0; featureId < 700; featureId++) {

          // split every feature at each split point.
          // e.g. is feature= 0.45 then at splitPoint 0.2 it goes to split 0; split point = 0.4 it goes to split 1.
          for (double splitPoint: splitPoints) {

            context.write(
                new DTKey(nodeId, true, 0, splitPoint, featureId)
                , new DTValue(0.0, 0)
            ); // In case no records lie in split 0
            context.write(
                new DTKey(nodeId, true, 1, splitPoint, featureId)
                , new DTValue(0.0, 0)
            ); // In case no records lie in split 1
            context.write(
                new DTKey(nodeId, false, 0, splitPoint, featureId)
                , new DTValue(0.0, 0)
            ); // In case no records lie in split 0
            context.write(
                new DTKey(nodeId, false, 1, splitPoint, featureId)
                , new DTValue(0.0, 0)
            ); // In case no records lie in split 1

            // Check if the current feature value is less than the current split point.
            if (r.getFeature(featureId) < splitPoint) {
              // Order inversion technique is used: dummy nodes are used to calculate the mean before encounter the nodes.
              DTKey dummyEmitKeyForSplit_0 = new DTKey(nodeId, true, 0, splitPoint, featureId);
              DTKey emitKey = new DTKey(nodeId, false, 0, splitPoint, featureId); // split 0 [left]
              DTValue emitValue = new DTValue(r.getRating().get(), 1);

              context.write(dummyEmitKeyForSplit_0, emitValue); // For calculating mean using order inversion
              context.write(emitKey, emitValue);
            }
            else {
              // Order inversion technique is used: dummy nodes are used to calculate the mean before encounter the nodes.
              DTKey dummyEmitKeyForSplit_1 = new DTKey(nodeId, true, 1, splitPoint, featureId);
              DTKey emitKey = new DTKey(nodeId, false, 1, splitPoint, featureId); // split 1 [right]
              DTValue emitValue = new DTValue(r.getRating().get(), 1);

              context.write(dummyEmitKeyForSplit_1, emitValue); // For calculating mean using order inversion
              context.write(emitKey, emitValue);
            }
          }
        }
      }
    }
  }

  // Custom combiner ot count the occurrences of ratings [0 and 1]
  public static class SplitCombiner extends Reducer<DTKey, DTValue, DTKey, DTValue> {

    @Override
    public void reduce(final DTKey key, final Iterable<DTValue> values, final Context context) throws IOException, InterruptedException {
      int maxRating = 1;

      // Possible predictions: {0, 1}
      // Counting occurrences each of ratings
      Map<Double, Integer> mp = new HashMap<>();

      // the required keys are the ratings from 0 and 1.
      for (double i = 0.0; i <= maxRating; i++) {
        mp.put(i, 0); // initialize the map
      }

      // count the number of ratings from the values.
      for (DTValue value: values) {
        mp.put(value.value.get(), mp.get(value.value.get()) + value.count.get());
      }

      // emit the counts in the map.
      for (double i = 0.0; i <= maxRating; i++) {
        context.write(key, new DTValue(i, mp.get(i))); // [node, (rating = i, count of rating = i)]
      }
    }
  }

  /**
   * Order Inversion Pattern:-
   * Order of Input:
   * Dummy data for mean of split_1
   * Data for variance of split_1
   * Dummy data for mean of split_2
   * Data for variance of split_2
   */
  public static class SplitReducer extends Reducer<DTKey, DTValue, Text, Text> {

    // stores the smallest variance for each node in the current level.
    private Map<Integer, Split> smallestVarianceMap;
    private double meanOfSplit_1;
    private double meanOfSplit_2;
    private int countOfSplit_1;
    private int countOfSplit_2;
    private double varianceOfSplit_1;
    private double varianceOfSplit_2;
    private boolean mean1_isSet;
    private boolean mean2_isSet;
    private boolean variance1_isSet;
    private boolean variance2_isSet;

    @Override
    public void setup(final Reducer.Context context) throws IOException {
      smallestVarianceMap = new HashMap<>();
      mean1_isSet = false;
      mean2_isSet = false;
      variance1_isSet = false;
      variance2_isSet = false;
    }

    @Override
    public void reduce(final DTKey key, final Iterable<DTValue> values, final Context context) {

      // Use dummies to find mean.
      if (key.dummy.get()) {

        double mean = 0.0;
        int count = 0;
        for (DTValue valueComps: values) {
          mean += valueComps.value.get() * valueComps.count.get(); // value = rating; count = count of each rating.
          count += valueComps.count.get();
        }
        if (count != 0) {
          mean /= count;
        }

        if (!mean1_isSet) {
          meanOfSplit_1 = mean;
          countOfSplit_1 = count;
          mean1_isSet = true;
        }
        else if (!mean2_isSet) {
          meanOfSplit_2 = mean;
          countOfSplit_2 = count;
          mean2_isSet = true;
        }
      }
      // If not dummy, then find variance.
      else {

        int nodeId = key.nodeId.get();

        if (mean2_isSet) {
          double variance = 0.0;
          for (DTValue valueComps: values) {
            variance += valueComps.count.get() * Math.pow(meanOfSplit_2 - valueComps.value.get(), 2);
          }
          if (countOfSplit_2 != 0) {
            variance = variance/countOfSplit_2;
          }
          varianceOfSplit_2 = variance;
          variance2_isSet = true;
        }
        else if (mean1_isSet) {
          double variance = 0.0;
          for (DTValue valueComps: values) {
            variance += valueComps.count.get() * Math.pow(meanOfSplit_1 - valueComps.value.get(), 2);
          }
          if (countOfSplit_1 != 0) {
            variance = variance/countOfSplit_1;
          }
          varianceOfSplit_1 = variance;
          variance1_isSet = true;
        }
        if (variance1_isSet && variance2_isSet) {

          double varianceForSplit;
          double mean;
          varianceForSplit = (varianceOfSplit_1 * countOfSplit_1 + varianceOfSplit_2 * countOfSplit_2) / (countOfSplit_1 + countOfSplit_2);
          mean = (meanOfSplit_1 * countOfSplit_1 + meanOfSplit_2 * countOfSplit_2) / (countOfSplit_1 + countOfSplit_2);

          // node with minimum variance
          if ( !smallestVarianceMap.containsKey(nodeId) || (smallestVarianceMap.containsKey(nodeId)
              && varianceForSplit < smallestVarianceMap.get(nodeId).variance )) {

            // checks if all the data is on one side of the split.
            Split split = new Split(nodeId, key.featureId.get(), key.splitPoint.get(), varianceForSplit, mean,
                countOfSplit_1 == 0 || countOfSplit_2 == 0);
            smallestVarianceMap.put(nodeId, split);
          }
          mean1_isSet = false;
          mean2_isSet = false;
          countOfSplit_1 = 0;
          countOfSplit_2 = 0;
          variance1_isSet = false;
          variance2_isSet = false;
        }
      }
    }

    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException {
      for (int nodeId: smallestVarianceMap.keySet()) {

        Split bestSplit = smallestVarianceMap.get(nodeId);

        Text key = new Text(
            nodeId + " " + bestSplit.featureId + " " + bestSplit.splitPoint
        );

        Text value = new Text(
            bestSplit.variance + " " + bestSplit.mean + " " + bestSplit.isSkewed
        );

        context.write(key, value);
      }
    }
  }

  /**
   * Grouping comparator to send all
   */
  public static class GroupingComparator extends WritableComparator {

    public GroupingComparator() {
      super(DTKey.class, true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {

      DTKey key1 = (DTKey) wc1;
      DTKey key2 = (DTKey) wc2;

      return key1.compareTo(key2);
    }
  }

  public static class processDataMapper extends Mapper<Object, Text, Record, NullWritable> {

    Map<Integer, Split> nodesPresent;

    @Override
    public void setup(final Context context) throws IOException {
      nodesPresent = new HashMap<>();
      Path[] filePaths = context.getLocalCacheFiles();
      for(Path filePath: filePaths) {
        FileInputStream fis = new FileInputStream(new File(filePath.toUri().getPath()));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line;
        while ((line = br.readLine()) != null && !line.equals("")) {
          System.out.println(line);

          Split split = new Split(line);
          int nodeId = split.nodeId;
          if (!split.isLeaf) {
            nodesPresent.put(nodeId, split);
          }
        }
      }
    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

      final StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        Record r = new Record(itr.nextToken());
        if (nodesPresent.containsKey(r.getNodeId())) {
          Split split = nodesPresent.get(r.getNodeId());
          if (r.getFeature(split.featureId) < split.splitPoint) {
            r.setNodeId(2*r.getNodeId()); // left child of a node in the tree
          }
          else {
            r.setNodeId(2*r.getNodeId() + 1); // right child of a node in the tree
          }
          context.write(r, null);
        }
      }
    }

  }

  // Job 1 Mapper : Read all the records and append node id = '1' at the start of each record.
  // This node id is further edited in the next job.
  public static class preProcessMapper extends Mapper<Object, Text, Record, NullWritable> {

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

      final StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        Record r = new Record("1 "+itr.nextToken()); // add node id = "1"
        context.write(r, null);
      }
    }
  }

  // Random Sampling of input data
  public static class Sampling extends Mapper<Object, Text, NullWritable, Text> {

    private final Random randomNumber = new Random();
    private Double samplingPercentage;

    protected void setup(Context context) throws IOException, InterruptedException {
      String percentage = context.getConfiguration().get("sampling_percentage");
      samplingPercentage = Double.parseDouble(percentage) / 100.0;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      if (randomNumber.nextDouble() < samplingPercentage) {
        context.write(NullWritable.get(), value);
      }
    }
  }

  public static void sampleJob(Configuration conf, String inputFile, String outputFile, double sampleSize)
          throws Exception {

    // Configuration
    final Job job = Job.getInstance(conf, "Decision Tree");
    job.setJarByClass(DecisionTree.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("sampling_percentage",String.valueOf(sampleSize));
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    job.setMapperClass(Sampling.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    job.waitForCompletion(true);
  }

  /**
   * Add node id to each record in the input file.
   * Map only job; No Reducer.
   *
   * @throws IOException due to context write.
   * @throws InterruptedException job failure.
   * @throws ClassNotFoundException from waitForCompletion.
   */
  public void preProcessJob() throws IOException, InterruptedException, ClassNotFoundException {

    // Job Configuration
    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Decision Tree");
    job.setJarByClass(DecisionTree.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", " ");

    // Job setup.
    job.setMapperClass(preProcessMapper.class);
    job.setOutputKeyClass(Record.class); // Record with node ids.
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(trainSample)); // Raw data file without node ids.
    FileOutputFormat.setOutputPath(job, new Path(levelData + "/1")); // Output file
    job.waitForCompletion(true);
  }

  // Represents each node after splits.
  public static class Split {
    int nodeId;
    int featureId;
    double splitPoint;
    double variance;
    double mean;
    boolean isSkewed;
    boolean isLeaf;

    public Split(int nodeId, int featureId, double splitPoint, double variance, double mean, boolean isSkewed) {
      this.nodeId = nodeId;
      this.featureId = featureId;
      this.splitPoint = splitPoint;
      this.variance = variance;
      this.mean = mean;
      this.isSkewed = isSkewed;
      this.isLeaf = false;
    }

    public Split(String line) {
      String[] data = line.split(" ");
      nodeId = Integer.parseInt(data[0]);
      featureId = Integer.parseInt(data[1]);
      splitPoint = Double.parseDouble(data[2]);
      variance = Double.parseDouble(data[3]);
      mean = Double.parseDouble(data[4]);
      isSkewed = Boolean.parseBoolean(data[5]);
      if (data.length == 7) {
        if (data[6].equals("true")) {
          isLeaf = true;
        }
        else {
          isLeaf = false;
        }
      }
    }

    public int getNodeId() {
      return nodeId;
    }

    public int getFeatureId() {
      return featureId;
    }

    public double getSplitPoint() {
      return splitPoint;
    }

    public double getMean() {
      return mean;
    }

    @Override
    public String toString() {
      return "" + nodeId + " " + featureId + " " + splitPoint + " " + variance + " " + mean + " " + isSkewed + " " + isLeaf;
    }
  }


  /**
   * Read treeLevel folder and emit (nodeId, node).
   */
  public static class DecideSplitMapper extends Mapper<Object, Text, IntWritable, Text> {
    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

      final StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

      while (itr.hasMoreTokens()) {
        Split s = new Split(itr.nextToken());
        context.write(new IntWritable(s.nodeId), new Text(s.toString()));
      }
    }
  }

  /**
   * For each nodeID, find the split with minimum variance.
   */
  public static class DecideSplitReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    Map<Integer, Split> id_split_map = new HashMap<>();

    @Override
    public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) {

      for (Text splitText: values) {
        Split split = new Split(splitText.toString());

        int nodeId = split.nodeId;

        // 1st occurrence of the node.
        if (!id_split_map.containsKey(nodeId)) {
          id_split_map.put(nodeId, split);
        }
        // find the minimum.
        else if (id_split_map.get(nodeId).variance > split.variance) {
          id_split_map.put(nodeId, split);
        }
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      // only emit nodes that have variance greater than this cap.
      double varianceCap = Double.parseDouble(context.getConfiguration().get("varianceCap"));

      // for each nodeID
      for (int nodeId: id_split_map.keySet()) {

        if (!id_split_map.get(nodeId).isSkewed && id_split_map.get(nodeId).variance > varianceCap) {
          context.getCounter(Counters.NodeCount).increment(1);
        }
        else {
          id_split_map.get(nodeId).isLeaf = true;
        }
        context.write(null, new Text(id_split_map.get(nodeId).toString()));
      }
    }
  }

  public boolean decideSplits(int layerCount) throws IOException, ClassNotFoundException, InterruptedException {

    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Best Splits");
    job.setJarByClass(DecisionTree.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", "");
    jobConf.set("varianceCap", String.valueOf(varianceCap));
    //jobConf.setInt("mapreduce.input.lineinputformat.linespermap", 4400);
    job.setMapperClass(DecideSplitMapper.class);
    job.setReducerClass(DecideSplitReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(treeLevel + "/" + layerCount));
    FileOutputFormat.setOutputPath(job, new Path(splits + "/" + layerCount));
    job.setNumReduceTasks(1);
    job.waitForCompletion(true);

    long count = job.getCounters().findCounter(Counters.NodeCount).getValue();

    return count != 0;
  }

  /**
   * Find the best features to split by for each node in the given layer of the tree.
   *
   * @param layerCount Maximum depth of the tree.
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  private void findBestSplitsJob(int layerCount) throws IOException, ClassNotFoundException, InterruptedException {

    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Best Splits");
    job.setJarByClass(DecisionTree.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", " ");

    job.setMapperClass(SplitMapper.class);
    job.setCombinerClass(SplitCombiner.class);
    job.setReducerClass(SplitReducer.class);
    job.setOutputKeyClass(DTKey.class);
    job.setOutputValueClass(DTValue.class);
    job.setGroupingComparatorClass(GroupingComparator.class);
    job.setCombinerKeyGroupingComparatorClass(GroupingComparator.class);

    FileInputFormat.addInputPath(job, new Path(levelData + "/" + layerCount));
    FileOutputFormat.setOutputPath(job, new Path(treeLevel + "/" + layerCount));

    job.waitForCompletion(true);
  }

  private void processDataForNextRound_Job(int layerCount) throws IOException, ClassNotFoundException, InterruptedException {

    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Data Processing for Best Splits");
    job.setJarByClass(DecisionTree.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", " ");
    //jobConf.setInt("mapreduce.input.lineinputformat.linespermap", 4400);
    job.setMapperClass(processDataMapper.class);
    job.setOutputKeyClass(Record.class);
    job.setOutputValueClass(NullWritable.class);
    //job.setInputFormatClass(FileInputFormat.class);
    job.setNumReduceTasks(0);
    job.addCacheFile(new Path(splits +"/"+layerCount+"/part-r-00000").toUri());
    FileInputFormat.addInputPath(job, new Path(levelData + "/" + layerCount));
    FileOutputFormat.setOutputPath(job, new Path(levelData + "/" + (layerCount + 1)));
    job.waitForCompletion(true);
  }

  /**
   * Trains the decision tree.
   * 1. sampleJob() : Randomly sample the input data.
   * 2. preProcessJob() : Add node ids to each record.
   * 3. findBestSplitsJob() : find the best features to split by for each node in the given layer of the tree.
   * 4. decideSplits() : for each node find the split with minimum variance.
   * 5. processDataForNextRound_Job() : Add nodes in each layer to separate files in levelData folder.
   *
   */
  @Override
  public int run(final String[] args) throws Exception {

    // Read Params
    trainInput = args[0];
    trainSample = args[2];
    levelData = args[4];
    treeLevel = args[5];
    splits = args[6];
    broadcastSplits = args[7];
    leafNodes = args[8];
    varianceCap = Double.parseDouble(args[9]); // ensure that the variance of the split is > 0.08 to avoid training data that is very similar to each other.
    maxDepth = Integer.parseInt(args[10]);
    sampleSize = Double.parseDouble(args[11]);

    // 1. Random sampling
    sampleJob(getConf(), trainInput, trainSample, sampleSize);

    // 2. Read data and append node id = 1 to each record.
    preProcessJob();

    int layerCount = 1; // depth of the node or the iteration of the tree.

    do {
      // 3. find the best features to split by for each node in the given layer of the tree.
      // Data is stored in splits data folder.
      findBestSplitsJob(layerCount);

      // 4: Decide Splits
      // If no more splits are possible
      if (!decideSplits(layerCount)) {
        break;
      }

      // 5. Change node ids such that a tree can be constructed.
      processDataForNextRound_Job(layerCount);

      layerCount++;

    }while (layerCount <= maxDepth); // limits the depth of the decision tree

    return 1;
  }

  /**
   * Main function that triggers the job.
   *
   * @param args 12 input arguments for the job.
   */
  public static void main(final String[] args) {
    if (args.length != 12) {
      throw new Error("12 arguments required");

    }
    try {
      //ToolRunner.run(new DecisionTree(), args); // Decision Tree Training Job.
      ToolRunner.run(new DecisionTreeTest(), args); // Decision Tree Testing Job
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}