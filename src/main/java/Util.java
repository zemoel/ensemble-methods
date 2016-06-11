import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.mllib.linalg.Vectors.dense;

/**
 * Created by pekasa on 09.06.16.
 */
public class Util {

    public double[][] arrayListdoubleArray(ArrayList<ArrayList<Double>> matrix, int numOfModels, int trainDataSetSize){
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

    public Vector listToVetor(List<?> list){
        List<Double> listDouble = (List<Double>) list;

        double[] doubleArray = new double[list.size()];
        for(int i= 0; i < listDouble.size(); i++){
            doubleArray[i] = listDouble.get(i);

        }
        Vector vector = dense(doubleArray);

        return vector;
    }

    public double[] listTodoubleArray(List<?> list){
        List<Double> listDouble = (List<Double>) list;

        double[] doubleArray = new double[list.size()];
        for(int i= 0; i < listDouble.size(); i++){
            doubleArray[i] = listDouble.get(i);

        }

        return doubleArray;
    }

    public double[] getLabels(JavaRDD<LabeledPoint> rddata){
        List<LabeledPoint> list = rddata.collect();
        // System.out.println("LEVEL0DATASET"+list);
        double[] labels = new double[list.size()];
        int i= 0;
        for(LabeledPoint item : list ){

            labels[i] = item.label();
            // System.out.println("LABELSONEBYONE"+labels[i]);
            i++;


        }

        return labels;
    }

    public double[] getLabelsFolds( Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>>[]  folds, int numOfDatapoints){
        double[] labels = new double[numOfDatapoints];
        int idx = 0;
        for(Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>> f : folds){
            for (LabeledPoint p : f._2().toJavaRDD().collect()){
                labels[idx] = p.label();
                idx++;
            }
        }
        return labels;
    }

    public List<LabeledPoint> matrixtoLabeledPoint(double[][] matrix, double[] labels){
        List<LabeledPoint> labeledList = new ArrayList<LabeledPoint>();
        LabeledPoint labeledRecord;

        for(int i=0; i<matrix.length; i++) {
            labeledRecord = new LabeledPoint(labels[i], Vectors.dense(matrix[i]));
            labeledList.add(labeledRecord);
        }
        return  labeledList;
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


    // Method to map feature vector to datapoints

    public Map<Vector, Integer> featureToIntegerMap(JavaRDD<LabeledPoint>trainDataSet){


        List<LabeledPoint> list = trainDataSet.collect();
        Map<Vector,Integer> featureVectorMap = new HashMap<Vector, Integer>();
        Integer mapValue = 0;
        for(LabeledPoint item : list ) {
            featureVectorMap.put(item.features(), mapValue);
            mapValue = mapValue.intValue() +1;

        }
        return featureVectorMap ;

    }

}
