/**
 * Created by pekasa on 05.05.16.
 */

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.util.SystemClock;


public class ReadCSV {
    // parse a csv file and convert it to JavaRDD

    public static void main(String[] args) throws IOException {
        ReadCSV.readCSV(" ");

    }

    public static void readCSV(String fileName) throws FileNotFoundException, IOException {
        SparkConf conf = new SparkConf().setAppName("Ensemble").setMaster("spark://myhost:7077");
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
            System.out.print(seenLabels.get(stringLabels));
            double[] doubleArray = new double[record.size()];
            for (int j = 0; j < record.size() - 1; j++) {
                Double d = Double.valueOf(record.get(j));
                doubleArray[j] = d;
            }


            LabeledPoint labeledRecord = new LabeledPoint(seenLabels.get(stringLabels),Vectors.dense(doubleArray));
            labeledList.add(i, labeledRecord);

        }


        JavaRDD<LabeledPoint> distData = sc.parallelize(labeledList);

    }
}

