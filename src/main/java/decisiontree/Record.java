package decisiontree;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Represents a record in the input.
 * Each record has an id, a rating and features along with their values.
 */
public class Record implements Writable {

  private IntWritable queryId;
  private IntWritable rating;
  private Map<IntWritable, DoubleWritable> features;
  private static DoubleWritable ZERO = new DoubleWritable(0.0);

  public Record() {
    this.queryId = new IntWritable();
    this.rating = new IntWritable();
    this.features = new HashMap<>();
  }

  public Record(int queryId, int rating) {
    this.queryId = new IntWritable(queryId);
    this.rating = new IntWritable(rating);
    this.features = new HashMap<>();
  }

  public Record(String input) {
    String[] parts = input.split(" ");
    queryId = new IntWritable(Integer.parseInt(parts[1].substring(4)));
    rating = new IntWritable(Integer.parseInt(parts[0]));
    this.features = new HashMap<>();
    for (int i = 2; i < parts.length; i++) {
      String[] id_val = parts[i].split(":");
      IntWritable featureId = new IntWritable(Integer.parseInt(id_val[0]));
      DoubleWritable featureValue = new DoubleWritable(Double.parseDouble(id_val[1]));
      this.features.put(featureId, featureValue);
    }
  }

  public void addFeature(int featureId, double value) {
    this.features.put(new IntWritable(featureId), new DoubleWritable(value));
  }

  public double getFeature(int featureId) {
    return this.features.getOrDefault(new IntWritable(featureId), ZERO).get();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {

    queryId.write(dataOutput);
    rating.write(dataOutput);
    new IntWritable(features.size()).write(dataOutput);

    for (Entry<IntWritable, DoubleWritable> feature: features.entrySet()) {
      feature.getKey().write(dataOutput);
      feature.getValue().write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

    queryId.readFields(dataInput);
    rating.readFields(dataInput);

    IntWritable countOfFeatures = new IntWritable();
    countOfFeatures.readFields(dataInput);
    this.features = new HashMap<>();

    for (int i = 0; i < countOfFeatures.get(); i++) {
      IntWritable featureId = new IntWritable();
      featureId.readFields(dataInput);
      DoubleWritable featureValue = new DoubleWritable();
      featureValue.readFields(dataInput);
      this.features.put(featureId, featureValue);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(rating.toString()).append(" qid:");
    sb.append(queryId.toString());
    for (Entry<IntWritable, DoubleWritable> feature: features.entrySet()) {
      IntWritable featureId = feature.getKey();
      DoubleWritable featureValue = feature.getValue();
      sb.append(" ")
          .append(featureId.toString())
          .append(":")
          .append(featureValue.toString());
    }
    return sb.toString();
  }

  public DoubleWritable getRating() {
    return new DoubleWritable(rating.get());
  }
}
