package decisiontree;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Represents a record in the input.
 * Each record has an id, a rating and features along with their values.
 */
public class Node implements Writable {

  private IntWritable nodeId;
  private IntWritable featureId;
  private DoubleWritable splitPoint;
  private DoubleWritable mean;
  private BooleanWritable isLeaf;
  private List<Node> children;

  public Node(String line) {
    String[] dataPoints = line.split(" ");
    this.nodeId = new IntWritable(Integer.parseInt(dataPoints[0]));
    this.featureId = new IntWritable(Integer.parseInt(dataPoints[1]));
    this.splitPoint = new DoubleWritable(Double.parseDouble(dataPoints[2]));
    this.isLeaf = new BooleanWritable();
    if (!dataPoints[3].isEmpty()) {
      isLeaf.set(true);
      this.mean = new DoubleWritable(Double.parseDouble(dataPoints[3]));
    }
    else {
      isLeaf.set(false);
    }
    this.children = new ArrayList<>();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    nodeId.write(dataOutput);
    featureId.write(dataOutput);
    splitPoint.write(dataOutput);
    isLeaf.write(dataOutput);
    if (isLeaf.get()) {
      mean.write(dataOutput);
    }
    new IntWritable(children.size()).write(dataOutput); // number of children
//    for (Node child: children) {
//      new IntWritable(child.nodeId.get()).write(dataOutput);
//      new IntWritable(child.featureId.get()).write(dataOutput);
//      new DoubleWritable(child.splitPoint.get()).write(dataOutput);
//      new BooleanWritable(child.isLeaf.get()).write(dataOutput);
//      if (child.isLeaf.get()) {
//        new DoubleWritable(child.mean.get()).write(dataOutput);
//      }
//    }
  }

  public Node() {
    nodeId = new IntWritable();
    featureId = new IntWritable();
    splitPoint = new DoubleWritable();
    isLeaf = new BooleanWritable();
    mean = new DoubleWritable();
    children = new ArrayList<>();
  }

  public void addChild(Node child) {
    if (child != null) {
      this.children.add(child);
    }
  }

  public List<Node> getChildren() {
    return new ArrayList<>(this.children);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

    nodeId.readFields(dataInput);
    featureId.readFields(dataInput);
    splitPoint.readFields(dataInput);
    isLeaf.readFields(dataInput);
    if (isLeaf.get()) {
      mean.readFields(dataInput);
    }
    IntWritable numOfChildren = new IntWritable();
    numOfChildren.readFields(dataInput);

//    this.children.clear();
//    for (int i = 0; i < numOfChildren.get(); i++) {
//      Node child = new Node();
//      child.nodeId.readFields(dataInput);
//      child.featureId.readFields(dataInput);
//      child.splitPoint.readFields(dataInput);
//      child.isLeaf.readFields(dataInput);
//      if (child.isLeaf.get()) {
//        child.mean.readFields(dataInput);
//      }
//
//    }
  }

  @Override
  public String toString() {

    StringBuilder sb = new StringBuilder();
    sb.append(nodeId.toString())
        .append(" ")
        .append(featureId.toString()).append(" ")
        .append(splitPoint.toString()).append(" ")
        .append(isLeaf.toString()).append(" ");
    if (this.isLeaf.get()) {
      sb.append(mean.toString());
    }
    return sb.toString();
  }
}
