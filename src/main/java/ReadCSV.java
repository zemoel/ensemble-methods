/**
 * Created by pekasa on 05.05.16.
 */


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ReadCSV {
     //parse a csv file and convert it to JavaRDD

   public static void main(String[] args) throws IOException {
        ReadCSV.buildBaseModels();

    }

    public static JavaRDD<LabeledPoint> readCSV() throws IOException {
        SparkConf conf = new SparkConf().setAppName("Ensemble").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //load and parse data

        Map<String, Integer> seenLabels = new HashMap<String, Integer>();

        Reader in = new FileReader("/home/pekasa/GSOC/iris.csv");
        CSVParser parser = CSVFormat.EXCEL.parse(in);
        List<CSVRecord> list = parser.getRecords();
        List<LabeledPoint> labeledList = new ArrayList<LabeledPoint>(list.size());


        for (int i = 0; i < list.size(); i++) {
            CSVRecord record = list.get(i);
            String stringLabels = record.get(record.size() - 1);
            if (!seenLabels.containsKey(stringLabels))
                seenLabels.put(stringLabels, seenLabels.size());

            double[] doubleArray = new double[record.size() - 1];
            for (int j = 0; j < record.size() - 1; j++)
                doubleArray[j] = Double.valueOf(record.get(j));



           LabeledPoint labeledRecord = new LabeledPoint(seenLabels.get(stringLabels), Vectors.dense(doubleArray));
           labeledList.add(labeledRecord);

        }



         JavaRDD<LabeledPoint> distData = sc.parallelize(labeledList);
        // System.out.print(distData);

        return  distData;


    }
    // create a method here to build models
    // train a model and return predictions
    // feed predictions to Stacking
    public static void buildBaseModels() throws IOException{
        JavaRDD<LabeledPoint> inputData = readCSV().cache();
        JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = tmp[0];
        JavaRDD<LabeledPoint> testingData = tmp[1];



}
}

