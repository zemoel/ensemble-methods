import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pekasa on 05.05.16.
 */


public class Stacking {

    public static void main(String[] args) throws IOException {
        Stacking s = new Stacking();
        Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>> datasets = s.preProcess();

        JavaRDD<LabeledPoint> training_data = datasets._1();
        JavaRDD<LabeledPoint> testing_data = datasets._2();
        ArrayList<String> models = new ArrayList<String>(2);
        models.add("DECISION_TREE");
        models.add("RANDOM_FOREST_CLASSIFICATION");
        s.train(training_data, models);

        //summary = s.test(testing_data, model);


    }
    public Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>> preProcess() throws IOException {
        ReadCSV read = new ReadCSV();
        JavaRDD<LabeledPoint> inputData = read.readCSV();
        JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.7, 0.3});

        return new Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>>(tmp[0], tmp[1]);
    }
    /**
     * This method trains an Stacking ensemble model
     *
     * @param  trainDataset            Training dataset as a JavaRDD of labeled points
     * @param  baseModels              List of basemodels selected for ensembling using Stacking
     * @param  numFolds
     * @param  seed
     * @return
     */

    public void train(JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels, Integer numFolds, Integer seed) throws  NullPointerException{


        RDD<LabeledPoint> r = trainDataset.rdd();
        ReadCSV build = new ReadCSV();


        Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>>[] folds =  MLUtils.kFold(r, numFolds, seed, trainDataset.classTag());
        JavaPairRDD<Double, Double> modelPredictions = null;
        List<Double> predictions = null;
        ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();


        double[][] level1Dataset = new double[(int)trainDataset.count()][baseModels.size()];//TODO: Cast not safe

        // TODO: For every Fold train and test a model passed as list of model, combine predictions, compute alpha
        for (String model: baseModels) {
            for (Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>> fold: folds) {


                try {
                     modelPredictions =  build.buildBaseModels(model, fold._1().toJavaRDD(),
                            fold._2().toJavaRDD());




                    modelPredictions.join(modelPredictions);
                    predictions = modelPredictions.keys().collect();
                    matrix.add((ArrayList<Double>) predictions);

                    //TODO: There must be a way to convert javapairrdd to matrix


                } catch (IOException e) {
                    e.printStackTrace();
                }

            }


        }
        for(int i= 0; i<matrix.size(); i++){
            ArrayList<Double> row = matrix.get(i);
            double[] copy = new double[row.size()];
            for(int j = 0; j< copy.length; j++){
                copy[j] = row.get(j);
            }
            level1Dataset[i] = copy;

        }



    }
    public void train(JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels){

        train(trainDataset,baseModels,2, 12345);

    }


    }


