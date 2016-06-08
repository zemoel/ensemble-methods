/**
 * Created by pekasa on 05.05.16.
 */


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ReadCSV{
     //parse a csv file and convert it to JavaRDD

    public static void main(String[] args) throws IOException {
       ReadCSV build = new ReadCSV();

       JavaRDD<LabeledPoint> rddata = build.readCSV();
       // build.parse();


    }

    public  JavaRDD<LabeledPoint> readCSV() throws IOException {



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


         JavaSparkContext sc = convertToRDD();
         JavaRDD<LabeledPoint> distData = sc.parallelize(labeledList);

        return  distData;


    }


    public Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>> preProcess() throws IOException {
        ReadCSV read = new ReadCSV();
        JavaRDD<LabeledPoint> inputData = read.readCSV();
        JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.7, 0.3});

        return new Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>>(tmp[0], tmp[1]);
    }



    public double[][] convertArrayListdoubleArray(ArrayList<ArrayList<Double>> matrix, int numOfModels, int trainDataSetSize){
        double[][] level1Dataset = new double[matrix.size()][matrix.get(0).size()];

        for(int i= 0; i<matrix.size(); i++){
            ArrayList<Double> row = matrix.get(i);
            double[] copy = new double[row.size()];
            for(int j = 0; j< copy.length; j++){
                copy[j] = row.get(j);
            }
            level1Dataset[i] = copy;

        }

        return level1Dataset ;
    }

    public Vector listToVector(List<?> list){
        List<Double> listDouble = (List<Double>) list;

        double[] doubleArray = new double[list.size()];
        for(int i= 0; i < listDouble.size(); i++){
            doubleArray[i] = listDouble.get(i);

        }
        Vector vector = Vectors.dense(doubleArray);

        return vector;
    }

    public List<String[]> LabeledpointToListStringArray( JavaRDD<LabeledPoint> rddata) {
        List<String[]> dataToBePredicted = new ArrayList<String[]>();
        List<LabeledPoint> list = rddata.collect();

        for(LabeledPoint item : list ){
            String[] labeledPointFeatures = new String[item.features().size()];
            double[] vector = item.features().toArray();

            for(int k= 0; k<vector.length; k++){
                labeledPointFeatures[k] = (Double.toString(vector[k]));


            }
            dataToBePredicted.add(labeledPointFeatures);

        }


        return dataToBePredicted;

    }


    public JavaSparkContext convertToRDD(){
        SparkConf conf = new SparkConf().setAppName("Ensemble").setMaster("local[2]");//.set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;

    }



}


