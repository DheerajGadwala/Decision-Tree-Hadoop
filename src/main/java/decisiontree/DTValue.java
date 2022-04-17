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
public class DTValue implements Writable {

  DoubleWritable value;
  IntWritable count;

  public DTValue() {
    value = new DoubleWritable();
    count = new IntWritable();
  }

  public DTValue(double value, int count) {
    this.value = new DoubleWritable(value);
    this.count = new IntWritable(count);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    value.write(dataOutput);
    count.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    value.readFields(dataInput);
    count.readFields(dataInput);
  }

  @Override
  public String toString() {
    return value.toString() + " " + count.toString();
  }
}
