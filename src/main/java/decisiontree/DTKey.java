package decisiontree;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static java.util.Objects.hash;

/**
 * Represents a record in the input.
 * Each record has an id, a rating and features along with their values.
 */
public class DTKey implements WritableComparable<DTKey> {

  BooleanWritable dummy;
  IntWritable split;
  DoubleWritable splitPoint;
  IntWritable featureId;

  public DTKey() {
    this.dummy = new BooleanWritable(false);
    this.split = new IntWritable(0);
    this.splitPoint = new DoubleWritable(0.0);
    this.featureId = new IntWritable(0);
  }

  public DTKey(boolean dummy, int split, double splitPoint, int featureId) {
    this.dummy = new BooleanWritable(dummy);
    this.split = new IntWritable(split);
    this.splitPoint = new DoubleWritable(splitPoint);
    this.featureId = new IntWritable(featureId);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dummy.write(dataOutput);
    split.write(dataOutput);
    splitPoint.write(dataOutput);
    featureId.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    dummy.readFields(dataInput);
    split.readFields(dataInput);
    splitPoint.readFields(dataInput);
    featureId.readFields(dataInput);
  }

  @Override
  public boolean equals(Object thatObject) {
    if (thatObject instanceof DTKey) {
      DTKey that = (DTKey) thatObject;
      return this.featureId.get() == that.featureId.get() &&
          this.split.get() == that.split.get() &&
          this.splitPoint.get() == that.splitPoint.get();
    }
    else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return hash(split.get(), splitPoint.get(), featureId.get());
  }

  @Override
  public int compareTo(DTKey that) {

    int splitCmp = this.split.compareTo(that.split);
    int splitPointCmp = this.splitPoint.compareTo(that.splitPoint);
    int featureIdCmp = this.featureId.compareTo(that.featureId);
    int dummyCmp = -this.dummy.compareTo(that.dummy);

    if (splitCmp == 0) {
      if (splitPointCmp == 0) {
        if (featureIdCmp == 0) {
          return dummyCmp;
        }
        else {
          return featureIdCmp;
        }
      }
      else {
        return splitPointCmp;
      }
    }
    else {
      return splitCmp;
    }
  }

  @Override
  public String toString() {
    return featureId.toString() + " " + splitPoint.toString() + " " + split.toString() + " " + dummy.toString();
  }
}
