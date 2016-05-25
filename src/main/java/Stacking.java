import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;

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
        s.train(training_data, models);

        //summary = s.test(testing_data, model);

        //s.train(set, models);
        //

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

    public void train(JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels, Integer numFolds, Integer seed){

        RDD<LabeledPoint> r = trainDataset.rdd();
        ReadCSV read = new ReadCSV();
        ReadCSV build = new ReadCSV();

        Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>>[] folds =  MLUtils.kFold(r, numFolds, seed, trainDataset.classTag());
        // TODO: For every Fold train and test a model passed as list of model, combine predictions, compute alpha
        for (Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>> fold: folds) {

            for (String model: baseModels) {
                try {
                    build.buildBaseModels(model, fold._1().toJavaRDD(), fold._2().toJavaRDD());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }

    }
    public void train(JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels){

        train(trainDataset,baseModels,6, 12345);

    }


    }


