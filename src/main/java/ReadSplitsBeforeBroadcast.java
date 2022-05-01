import decisiontree.DecisionTree;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Read data from splits and add the data into a single file.
 * Run this program locally before testing decision tree.
 */
public class ReadSplitsBeforeBroadcast {

    private static void combineSplitFiles(int layerCount) throws Exception {
        // Splits folder will have layer count number of files.
        // read all those files and put the data into one single file

        // Input files
        String inFile = "splits/";

        // Output files
        File outFile = new File("broadcastSplits/data");
        outFile.getParentFile().mkdirs(); // creates the directory "output"
        outFile.createNewFile();
        FileWriter fWriter = new FileWriter(outFile);

        // cache the depth of the tree so that we can create an array of size (2^n) to construct a tree.
        fWriter.write(layerCount + "\n");
        int r = 1;

        // to iterate through the intermediate output files.
        while (r <= layerCount) {
            // Input file
            File splitNodes = new File(inFile + r +"/part-r-00000");
            BufferedReader brSplit = new BufferedReader(new FileReader(splitNodes));
            String line;

            while ((line = brSplit.readLine()) != null && !line.equals("")) {
                DecisionTree.Split split = new DecisionTree.Split(line);
                fWriter.write("" + split.getNodeId() + " " + split.getFeatureId() + " " + split.getSplitPoint() + " " + split.getMean() + "\n");
            }
            r++;
        }
        fWriter.close();
    }

    public static void main(String[] args) throws Exception {
        combineSplitFiles(15);
    }
}
