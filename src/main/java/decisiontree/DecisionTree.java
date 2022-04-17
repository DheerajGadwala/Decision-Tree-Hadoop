package decisiontree;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

    double[] splitPoints = new double[] {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
      System.out.println("mapperDheeraj");
      final StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {

        Record r = new Record(itr.nextToken());

        for (int featureId = 0; featureId < 700; featureId++) {
          for (double splitPoint: splitPoints) {
            if (r.getFeature(featureId) < splitPoint) {

              DTKey dummyEmitKeyForSplit_0 = new DTKey(true, 0, splitPoint, featureId);
              DTKey emitKey = new DTKey(false, 0, splitPoint, featureId);
              DTValue emitValue = new DTValue(r.getRating().get(), 1);

              context.write(dummyEmitKeyForSplit_0, emitValue); // For calculating mean using order inversion
              context.write(emitKey, emitValue);
            }
            else {

              DTKey dummyEmitKeyForSplit_1 = new DTKey(true, 1, splitPoint, featureId);
              DTKey emitKey = new DTKey(false, 1, splitPoint, featureId);
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
      double sumValue = 0.0;
      int sumCount = 0;

      for (DTValue value: values) {
        sumValue += value.value.get();
        sumCount += value.count.get();
      }

      context.write(key, new DTValue(sumValue, sumCount));
    }
  }

  public static class SplitReducer extends Reducer<DTKey, DTValue, DTKey, DTValue> {

    double mean;
    int count;

    @Override
    public void reduce(final DTKey key, final Iterable<DTValue> values, final Context context) throws IOException, InterruptedException {
      System.out.println(key);
      if (key.dummy.get()) {
        mean = 0.0;
        count = 0;
        for (DTValue valueComps: values) {
          mean += valueComps.value.get() * valueComps.count.get();
          count += valueComps.count.get();
        }
      }
      else {
        double variance = 0.0;
        for (DTValue valueComps: values) {
          variance += Math.pow(mean - valueComps.value.get(), 2);
        }
        variance = Math.sqrt(variance/count);
        context.write(key, new DTValue(variance, count));
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

      int splitCmp = key1.split.compareTo(key2.split);
      int splitPointCmp = key1.splitPoint.compareTo(key2.splitPoint);
      int featureIdCmp = key1.featureId.compareTo(key2.featureId);
      int dummyCmp = -key1.dummy.compareTo(key2.dummy);

      if (featureIdCmp == 0) {
        if (splitPointCmp == 0) {
          if (splitCmp == 0) {
            return dummyCmp;
          }
          else {
            return splitCmp;
          }
        }
        else {
          return splitPointCmp;
        }
      }
      else {
        return featureIdCmp;
      }
    }
  }


  @Override
  public int run(final String[] args) throws Exception {
    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Decision Tree");
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

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
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