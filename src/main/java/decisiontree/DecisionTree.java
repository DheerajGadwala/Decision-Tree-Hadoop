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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DecisionTree extends Configured implements Tool {

  String inputFolder, levelDataFolder, treeLevelFolder,
      splitsFolder, broadcastSplits, leafNodesFolder, sampleFolder;

  double varianceCap;

  private static final Logger logger = LogManager.getLogger(DecisionTree.class); // log

  // Job 2 Mapper: Split the data based on each split point for every feature.
  //              1. read the data and create an object using class Record.
  //              2. define split points for the features [it is equally spaced values]
  //              3. for each feature -> split the feature by each split point.
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
        for (int featureId = 0; featureId < 23; featureId++) {

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

            if (r.getFeature(featureId) < splitPoint) {

              // Order inversion technique is used: dummy nodes are used to calculate the mean before encounter the nodes.
              DTKey dummyEmitKeyForSplit_0 = new DTKey(nodeId, true, 0, splitPoint, featureId);
              DTKey emitKey = new DTKey(nodeId, false, 0, splitPoint, featureId);
              DTValue emitValue = new DTValue(r.getRating().get(), 1);

              context.write(dummyEmitKeyForSplit_0, emitValue); // For calculating mean using order inversion
              context.write(emitKey, emitValue);
            }
            else {

              // Order inversion technique is used: dummy nodes are used to calculate the mean before encounter the nodes.
              DTKey dummyEmitKeyForSplit_1 = new DTKey(nodeId, true, 1, splitPoint, featureId);
              DTKey emitKey = new DTKey(nodeId, false, 1, splitPoint, featureId);
              DTValue emitValue = new DTValue(r.getRating().get(), 1);

              context.write(dummyEmitKeyForSplit_1, emitValue); // For calculating mean using order inversion
              context.write(emitKey, emitValue);
            }
          }
        }
      }
    }
  }

  // Custom Combiner also uses grouping comparator.
  public static class SplitCombiner extends Reducer<DTKey, DTValue, DTKey, DTValue> {

    @Override
    public void reduce(final DTKey key, final Iterable<DTValue> values, final Context context) throws IOException, InterruptedException {

      // Possible predictions: {0, 1, 2, 3, 4}
      // Counting occurrences each of ratings
      Map<Double, Integer> mp = new HashMap<>();

      // the required keys are the ratings from 0 to 4.
      for (double i = 0.0; i <= 4; i++) {
        mp.put(i, 0); // initialize the map
      }

      // count the number of ratings from the values.
      for (DTValue value: values) {
        mp.put(value.value.get(), mp.get(value.value.get()) + value.count.get());
      }

      // emit the counts in the map.
      for (double i = 0.0; i <= 4; i++) {
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
          mean += valueComps.value.get() * valueComps.count.get();
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

          double varianceForSplit = (
              varianceOfSplit_1 * countOfSplit_1
                  + varianceOfSplit_2 * countOfSplit_2
          ) / (countOfSplit_1 + countOfSplit_2);

          double mean = (
              meanOfSplit_1 * countOfSplit_1
                  + meanOfSplit_2 * countOfSplit_2
          ) / (countOfSplit_1 + countOfSplit_2);

          // node with minimum variance
          if ( !smallestVarianceMap.containsKey(nodeId) || (smallestVarianceMap.containsKey(nodeId)
                  && varianceForSplit < smallestVarianceMap.get(nodeId).variance )) {

            Split split = new Split(nodeId, key.featureId.get(), key.splitPoint.get(), varianceForSplit, mean,
                countOfSplit_1 == 0 || countOfSplit_2 == 0); // checks if all the data is on one side of the split.
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

  public static class GroupingComparator extends WritableComparator {

    public GroupingComparator() {
      super(DTKey.class, true);
    } // Todo: what is createInstances?

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
          Split split = new Split(line);
          int nodeId = split.nodeId;
          nodesPresent.put(nodeId, split);
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
//        if (r.getRating().get() >= 3) {
//          r.setRating(1);
//        }
//        else {
//          r.setRating(0);
//        }
        context.write(r, null);
      }
    }
  }

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

  public void sampleJob() throws Exception {
    // Configuration
    final Configuration conf0 = getConf();
    final Job job0 = Job.getInstance(conf0, "Decision  Tree");
    job0.setJarByClass(DecisionTree.class);
    final Configuration jobConf0 = job0.getConfiguration();
    jobConf0.set("sampling_percentage","20");
    FileInputFormat.addInputPath(job0, new Path(inputFolder));
    FileOutputFormat.setOutputPath(job0, new Path(sampleFolder));
    job0.setMapperClass(Sampling.class);
    job0.setOutputKeyClass(NullWritable.class);
    job0.setOutputValueClass(Text.class);
    job0.setNumReduceTasks(0);
    job0.waitForCompletion(true);

  }


  // Job - 1
  public void preProcessJob() throws Exception {


    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Decision Tree");
    job.setJarByClass(DecisionTree.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", " ");
    //jobConf.setInt("mapreduce.input.lineinputformat.linespermap", 1000);

    job.setMapperClass(preProcessMapper.class);
    job.setOutputKeyClass(Record.class);
    job.setOutputValueClass(NullWritable.class);
    //job.setInputFormatClass(NLineInputFormat.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(sampleFolder));
    FileOutputFormat.setOutputPath(job, new Path(levelDataFolder + "/1"));

    job.waitForCompletion(true);
  }

  private static class Split {
    int nodeId;
    int featureId;
    double splitPoint;
    double variance;
    double mean;
    boolean isSkewed;


    Split(int nodeId, int featureId, double splitPoint, double variance, double mean, boolean isSkewed) {
      this.nodeId = nodeId;
      this.featureId = featureId;
      this.splitPoint = splitPoint;
      this.variance = variance;
      this.mean = mean;
      this.isSkewed = isSkewed;
    }

    Split(String line) {
      String[] data = line.split(" ");
      nodeId = Integer.parseInt(data[0]);
      featureId = Integer.parseInt(data[1]);
      splitPoint = Double.parseDouble(data[2]);
      variance = Double.parseDouble(data[3]);
      mean = Double.parseDouble(data[4]);
      isSkewed = Boolean.parseBoolean(data[5]);
    }

    @Override
    public String toString() {
      return "" + nodeId + " " + featureId + " " + splitPoint + " " + variance + " " + mean + " " + isSkewed + "\n";
    }
  }

  // Serialized computation.
  public boolean decideSplits(int numberOfReduceTasks, int layerCount) throws IOException {

    Map<Integer, Split> id_split_map = new HashMap<>();

    String fileName = treeLevelFolder + "/" + layerCount + "/part-r-";

    int r = 0;
    // to iterate through the intermediate output files.
    while (r < numberOfReduceTasks) {

      StringBuilder is = new StringBuilder(String.valueOf(r));

      while(is.length() != 5) {
        is.insert(0, "0");
      }

      // get file
      File file = new File(fileName + is.toString());

      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;
      while ((line = br.readLine()) != null && !line.equals("")) {

        Split split = new Split(line);
        int nodeId = split.nodeId;

        if (!id_split_map.containsKey(nodeId)) {
          id_split_map.put(nodeId, split);
        }
        else if (id_split_map.get(nodeId).variance > split.variance) {
          id_split_map.put(nodeId, split);
        }
      }
      r++;
    }

    File outFile = new File(splitsFolder + "/" + layerCount);
    outFile.getParentFile().mkdirs();
    outFile.createNewFile();
    FileWriter fWriter = new FileWriter(outFile);
    File leafNodes = new File(leafNodesFolder + "/" + layerCount);
    leafNodes.getParentFile().mkdirs();
    leafNodes.createNewFile();
    FileWriter fWriterForLeafs = new FileWriter(leafNodes);
    boolean continueProcessing = false;

    for (int nodeId: id_split_map.keySet()) {
      if (!id_split_map.get(nodeId).isSkewed && id_split_map.get(nodeId).variance > varianceCap) {
        fWriter.write(id_split_map.get(nodeId).toString());
        continueProcessing = true;
      }
      else {
        fWriterForLeafs.write(id_split_map.get(nodeId).toString());
      }
    }
    fWriter.close();
    fWriterForLeafs.close();

    return continueProcessing;
  }

  private boolean findBestSplitsJob(int layerCount) throws IOException, ClassNotFoundException, InterruptedException {

    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Best Splits");
    job.setJarByClass(DecisionTree.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", " ");
    //jobConf.setInt("mapreduce.input.lineinputformat.linespermap", 4400);
    job.setMapperClass(SplitMapper.class);
    job.setCombinerClass(SplitCombiner.class);
    job.setReducerClass(SplitReducer.class);
    job.setOutputKeyClass(DTKey.class);
    job.setOutputValueClass(DTValue.class);
    job.setGroupingComparatorClass(GroupingComparator.class);
    job.setCombinerKeyGroupingComparatorClass(GroupingComparator.class);
    //job.setInputFormatClass(FileInputFormat.class);
    //job.setInputFormatClass(NLineInputFormat.class);
    //job.setNumReduceTasks(5);
    //NLineInputFormat.addInputPath(job, new Path(args[1] + "/" + layerCount));
    FileInputFormat.addInputPath(job, new Path(levelDataFolder + "/" + layerCount));
    FileOutputFormat.setOutputPath(job, new Path(treeLevelFolder + "/" + layerCount));

    job.waitForCompletion(true);

    return decideSplits(job.getNumReduceTasks(), layerCount);
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
    job.addCacheFile(new Path(splitsFolder+"/"+layerCount).toUri());
    FileInputFormat.addInputPath(job, new Path(levelDataFolder + "/" + layerCount));
    FileOutputFormat.setOutputPath(job, new Path(levelDataFolder + "/" + (layerCount + 1)));
    job.waitForCompletion(true);
  }

  private void ReadSplitsBeforeBroadcast(int layerCount) throws Exception {
    // Splits folder will have layer count number of files.
    // read all those files and put the data into one single file

    // Input files
    String inFile = splitsFolder + "/";
    String leafFile = leafNodesFolder + "/";

    // Output files
    File outFile = new File(broadcastSplits+"/data");
    outFile.getParentFile().mkdirs(); // creates the directory "output"
    outFile.createNewFile();
    FileWriter fWriter = new FileWriter(outFile);

    // cache the depth of the tree so that we can create an array of size (2^n) to construct a tree.
    fWriter.write(layerCount + "\n");
    int r = 1;
    // to iterate through the intermediate output files.
    while (r <= layerCount) {

      // Input file
      File splitNodes = new File(inFile + r);
      File leafNodes = new File(leafFile + r);

      BufferedReader brSplit = new BufferedReader(new FileReader(splitNodes));
      BufferedReader brLeaf = new BufferedReader(new FileReader(leafNodes));
      String line;

      while ((line = brSplit.readLine()) != null && !line.equals("")) {
        Split split = new Split(line);
          fWriter.write("" + split.nodeId + " " + split.featureId + " " + split.splitPoint + " " + split.mean + "\n");
      }

      while ((line = brLeaf.readLine()) != null && !line.equals("")) {
        Split split = new Split(line);
          fWriter.write("" + split.nodeId + " " + split.featureId + " " + split.splitPoint + " " + split.mean + "\n");
      }
      r++;
    }
    fWriter.close();
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
    int maxDepth = Integer.parseInt(args[7]);
    sampleFolder = args[8];

    //Job 0 : handles sampling
    sampleJob();

    // Job 1 : Read data and append node id = 1 to each record.
    preProcessJob();

    int layerCount = 1; // depth of the node or the iteration of the tree.

    boolean continueProcessing;

    do {
      // Job 2: find the best features to split by for each node in the given layer of the tree.
      // Data is stored in splits data folder.
      continueProcessing = findBestSplitsJob(layerCount);

      if (!continueProcessing) {
        break;
      }

      // Job 3: Add nodes in each layer to files in layersDataFolder.
      processDataForNextRound_Job(layerCount);

      layerCount++;

    }while (layerCount <= maxDepth); // limits the depth of the decision tree

    // Read the files from splitsFolder and put it in single file.
    ReadSplitsBeforeBroadcast(layerCount);
    return layerCount;
  }

  public static void main(final String[] args) {
    if (args.length != 9) {
      throw new Error("Nine arguments required");
    }
    try {
      ToolRunner.run(new DecisionTree(), args);
      //ToolRunner.run(new DecisionTreeTest(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}