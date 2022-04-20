package decisiontree;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DecisionTree extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(DecisionTree.class);

  public static class SplitMapper extends Mapper<Object, Text, DTKey, DTValue> {

    double[] splitPoints = new double[] {0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

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

    Map<Integer, DTKey> smallestKeyMap = new HashMap<>();
    Map<Integer, Double> smallestVarianceMap = new HashMap<>();
    Map<Integer, Double> smallestMeanMap = new HashMap<>();

    Double meanOfSplit_1;
    Double meanOfSplit_2;
    Integer countOfSplit_1;
    Integer countOfSplit_2;
    Double varianceOfSplit_1;
    Double varianceOfSplit_2;

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

        if (meanOfSplit_1 == null) {
          meanOfSplit_1 = mean;
          countOfSplit_1 = count;
        }
        else if (meanOfSplit_2 == null) {
          meanOfSplit_2 = mean;
          countOfSplit_2 = count;
        }
      }
      // If not dummy, then find variance.
      else {

        int nodeId = key.nodeId.get();

        if (meanOfSplit_2 != null) {
          double variance = 0.0;
          for (DTValue valueComps: values) {
            variance += valueComps.count.get() * Math.pow(meanOfSplit_2 - valueComps.value.get(), 2);
          }
          if (countOfSplit_2 != 0) {
            variance = variance/countOfSplit_2;
          }
          varianceOfSplit_2 = variance;
        }
        else if (meanOfSplit_1 != null) {
          double variance = 0.0;
          for (DTValue valueComps: values) {
            variance += valueComps.count.get() * Math.pow(meanOfSplit_1 - valueComps.value.get(), 2);
          }
          if (countOfSplit_1 != 0) {
            variance = variance/countOfSplit_1;
          }
          varianceOfSplit_1 = variance;
        }
        if (varianceOfSplit_1 != null && varianceOfSplit_2 != null) {
          double varianceForSplit = (
              varianceOfSplit_1 * countOfSplit_1 + varianceOfSplit_2 * countOfSplit_2
          ) / (countOfSplit_1 + countOfSplit_2);
          if (varianceForSplit < smallestVarianceMap.getOrDefault(nodeId, Double.MAX_VALUE)) {

            double mean = (
                meanOfSplit_1 * countOfSplit_1 + meanOfSplit_2 * countOfSplit_2
            ) / (countOfSplit_1 + countOfSplit_2);

            smallestVarianceMap.put(nodeId, varianceForSplit);
            smallestMeanMap.put(nodeId, mean);
            smallestKeyMap.put(nodeId, key);
          }
          meanOfSplit_1 = null;
          meanOfSplit_2 = null;
          countOfSplit_1 = null;
          countOfSplit_2 = null;
          varianceOfSplit_1 = null;
          varianceOfSplit_2 = null;
        }
      }
    }

    @Override
    public void cleanup(final Context context) throws IOException, InterruptedException {
      for (int nodeId: smallestKeyMap.keySet()) {

        DTKey smallestKey = smallestKeyMap.get(nodeId);

        Text key = new Text(
            nodeId + " " + smallestKey.featureId + " " + smallestKey.splitPoint
        );
        Text value = new Text(
            smallestVarianceMap.get(nodeId) + " " + smallestMeanMap.get(nodeId)
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

  public static class preProcessMapper extends Mapper<Object, Text, Record, NullWritable> {

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

      final StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        Record r = new Record("1 "+itr.nextToken());
        context.write(r, null);
      }
    }

  }


  public void preProcessJob(final String[] args) throws Exception {

    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Decision Tree");
    job.setJarByClass(DecisionTree.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", " ");
    jobConf.setInt("mapreduce.input.lineinputformat.linespermap", 1000);

    job.setMapperClass(preProcessMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setInputFormatClass(NLineInputFormat.class);
    job.setNumReduceTasks(0);
    NLineInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("node/1"));

    job.waitForCompletion(true);
  }


  @Override
  public int run(final String[] args) throws Exception {

    preProcessJob(args);

    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Decision Tree");
    job.setJarByClass(DecisionTree.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", " ");
    jobConf.setInt("mapreduce.input.lineinputformat.linespermap", 4400);

    job.setMapperClass(SplitMapper.class);
    job.setCombinerClass(SplitCombiner.class);
    job.setReducerClass(SplitReducer.class);
    job.setOutputKeyClass(DTKey.class);
    job.setOutputValueClass(DTValue.class);
    job.setGroupingComparatorClass(GroupingComparator.class);
    job.setCombinerKeyGroupingComparatorClass(GroupingComparator.class);
    job.setInputFormatClass(NLineInputFormat.class);
    job.setNumReduceTasks(5);
    NLineInputFormat.addInputPath(job, new Path("node/1"));

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    return 1;
  }

  public static void main(final String[] args) {
    if (args.length != 2) {
      throw new Error("Two arguments required:\n<input-dir> <output-dir>");
    }
    try {
      ToolRunner.run(new DecisionTree(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}