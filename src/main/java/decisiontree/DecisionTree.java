package decisiontree;

import java.io.*;
import java.util.*;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DecisionTree extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(DecisionTree.class);

  public static class SplitMapper extends Mapper<Object, Text, DTKey, DTValue> {

    double[] splitPoints = new double[] {0.25, 0.5, 0.75};

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

      final StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {

        Record r = new Record(itr.nextToken());
        int nodeId = r.getNodeId();

        for (int featureId = 0; featureId < 700; featureId++) {
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

              DTKey dummyEmitKeyForSplit_0 = new DTKey(nodeId, true, 0, splitPoint, featureId);
              DTKey emitKey = new DTKey(nodeId, false, 0, splitPoint, featureId);
              DTValue emitValue = new DTValue(r.getRating().get(), 1);

              context.write(dummyEmitKeyForSplit_0, emitValue); // For calculating mean using order inversion
              context.write(emitKey, emitValue);
            }
            else {

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

  public static class SplitCombiner extends Reducer<DTKey, DTValue, DTKey, DTValue> {

    @Override
    public void reduce(final DTKey key, final Iterable<DTValue> values, final Context context) throws IOException, InterruptedException {

      // Possible predictions: {0, 1, 2, 3, 4}
      // Counting occurrences each of ratings
      Map<Double, Integer> mp = new HashMap<>();
      for (double i = 0.0; i <= 4; i++) {
        mp.put(i, 0);
      }
      for (DTValue value: values) {
        mp.put(value.value.get(), mp.get(value.value.get()) + value.count.get());
      }
      for (double i = 0.0; i <= 4; i++) {
        context.write(key, new DTValue(i, mp.get(i)));
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

    Map<Integer, Split> smallestVarianceMap = new HashMap<>();

    double meanOfSplit_1;
    double meanOfSplit_2;
    int countOfSplit_1;
    int countOfSplit_2;
    double varianceOfSplit_1;
    double varianceOfSplit_2;
    boolean mean1_isSet = false;
    boolean mean2_isSet = false;
    boolean variance1_isSet = false;
    boolean variance2_isSet = false;

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

          if (
              !smallestVarianceMap.containsKey(nodeId)
                  ||
              (smallestVarianceMap.containsKey(nodeId)
                  &&
              varianceForSplit < smallestVarianceMap.get(nodeId).variance
              )
          ) {
            Split split = new Split(
                nodeId, key.featureId.get(),
                key.splitPoint.get(), varianceForSplit, mean,
                countOfSplit_1 == 0 || countOfSplit_2 == 0
            );
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
            bestSplit.variance + " " + bestSplit.nodeId + " " + bestSplit.isPure
        );

        context.write(key, value);
      }
    }
  }

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
            r.setNodeId(2*r.getNodeId());
          }
          else {
            r.setNodeId(2*r.getNodeId()+1);
          }
          context.write(r, null);
        }
      }
    }

  }

  public static class preProcessMapper extends Mapper<Object, Text, Record, NullWritable> {

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

      final StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        Record r = new Record("1 "+itr.nextToken());
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


  public void preProcessJob(String inputFolder) throws Exception {

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

    FileInputFormat.addInputPath(job, new Path(inputFolder));
    FileOutputFormat.setOutputPath(job, new Path("layer/1"));

    job.waitForCompletion(true);
  }

  private static class Split {
    int nodeId;
    int featureId;
    double splitPoint;
    double variance;
    double mean;
    boolean isPure;

    Split(int nodeId, int featureId, double splitPoint, double variance, double mean, boolean isPure) {
      this.nodeId = nodeId;
      this.featureId = featureId;
      this.splitPoint = splitPoint;
      this.variance = variance;
      this.mean = mean;
      this.isPure = isPure;
    }

    Split(String line) {
      String[] data = line.split(" ");
      nodeId = Integer.parseInt(data[0]);
      featureId = Integer.parseInt(data[1]);
      splitPoint = Double.parseDouble(data[2]);
      variance = Double.parseDouble(data[3]);
      mean = Double.parseDouble(data[4]);
      isPure = Boolean.parseBoolean(data[5]);
    }

    @Override
    public String toString() {
      return "" + nodeId + " " + featureId + " " + splitPoint + " " + variance + " " + mean + " " + isPure + "\n";
    }
  }

  public boolean decideSplits(String treeLevelFolder, String splitsFolder, double varianceCap, int numberOfReduceTasks, int layerCount) throws IOException {

    Map<Integer, Split> id_split_map = new HashMap<>();

    String fileName = treeLevelFolder + "/" + layerCount + "/part-r-";

    int r = 0;
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

        if (!split.isPure) {
          if (!id_split_map.containsKey(nodeId)) {
            id_split_map.put(nodeId, split);
          }
          else if (id_split_map.get(nodeId).variance > split.variance) {
            id_split_map.put(nodeId, split);
          }
        }
      }
      r++;
    }

    File file = new File(splitsFolder + "/" + layerCount);
    file.getParentFile().mkdirs();
    file.createNewFile();
    FileWriter fWriter = new FileWriter(file);
    boolean continueProcessing = false;

    for (int nodeId: id_split_map.keySet()) {
      if (id_split_map.get(nodeId).variance > varianceCap) {
        fWriter.write("" + id_split_map.get(nodeId).toString());
        continueProcessing = true;
      }
    }
    fWriter.close();

    return continueProcessing;
  }

  private boolean findBestSplitsJob(String levelDataFolder, String treeLevelFolder, String splitsFolder, double varianceCap, int layerCount) throws IOException, ClassNotFoundException, InterruptedException {

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

    return decideSplits(treeLevelFolder, splitsFolder, varianceCap, job.getNumReduceTasks(), layerCount);
  }

  private void processDataForNextRound_Job(String levelDataFolder, String splitsFolder, int layerCount) throws IOException, ClassNotFoundException, InterruptedException {

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


  @Override
  public int run(final String[] args) throws Exception {

    // Read Params
    String inputFolder = args[0], levelDataFolder = args[1],
        treeLevelFolder = args[2], splitsFolder = args[3];
    double varianceCap = Double.parseDouble(args[4]);

    preProcessJob(inputFolder);

    int layerCount = 1;

    boolean continueProcessing;

    do {

      continueProcessing = findBestSplitsJob(
          levelDataFolder, treeLevelFolder,
          splitsFolder, varianceCap, layerCount
      );

      if (!continueProcessing) {
        break;
      }

      processDataForNextRound_Job(levelDataFolder, splitsFolder, layerCount);

      layerCount++;

    }while (true);
    return 1;
  }

  public static void main(final String[] args) {
    if (args.length != 4) {
      throw new Error("Two arguments required:\n<input-dir> <output-dir>");
    }
    try {
      ToolRunner.run(new DecisionTree(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}