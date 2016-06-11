import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.wso2.carbon.ml.commons.domain.MLModel;
import org.wso2.carbon.ml.core.exceptions.MLModelBuilderException;
import org.wso2.carbon.ml.core.exceptions.MLModelHandlerException;
import org.wso2.carbon.ml.core.impl.Predictor;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pekasa on 01.06.16.
 */
public class Stacking implements Serializable {

    private MLModel levelOneModel;
    private List<MLModel> levelZeroModels = new ArrayList<MLModel>();

    public Stacking() {

    }

    /**
     * This method trains an ComputeWeightsQPSt ensemble model
     *
     * @param trainDataset Training dataset as a JavaRDD of labeled points
     * @param baseModels   List of basemodels selected for ensembling using ComputeWeightsQPSt
     * @param numFolds
     * @param seed
     * @return
     */

    public void train(long modelId, JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels,
                      Integer numFolds, Integer seed) throws NullPointerException, MLModelHandlerException,
            MLModelBuilderException {

        // Step1. train list of level0models on cvdata - DONE
        // Step2. get predictions of each List<?> and combine predictions to get level1 dataset -DONE
        // Step3. train level1model on level1 dataset
        // Step4. train level0models on whole dataset and store list of models

        //TODO: Find a way to call this two statements from a central place maybe constructor?
        Util convert = new Util();
        BaseModelsBuilder build = new BaseModelsBuilder();


        RDD<LabeledPoint> r = trainDataset.rdd();

        // create a map from feature vector to some index. use this index in folds to track which datapoint is being predicted.

        Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>>[] folds = MLUtils.kFold(r, numFolds, seed,
                trainDataset.classTag());
        double[][] matrix = new double[(int) trainDataset.count()][baseModels.size()];


        // JavaRDD<LabeledPoint> validationData
        int cnt = 0;
        for (String model : baseModels) {

            int idx = 0;
            // train basemodels on whole dataset
            MLModel noCVbaseModel = build.buildBaseModels(model, trainDataset);
            // train basemodels on cross-validated data
            for (Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>> fold : folds) {
                List<String[]> dataTobePredicted = convert.LabeledpointToListStringArray(fold._2().toJavaRDD());
                System.out.println("SIZES: " + fold._1().count() + ", " + fold._2().count());
                // TODO: make sure that the RDD we train on is larger than the RDD we test on
                MLModel baseModel = build.buildBaseModels(model, fold._1.toJavaRDD());
                Predictor predictor = new Predictor(modelId, baseModel, dataTobePredicted);
                List<?> predictions = predictor.predict();
                double[] doubleArrayPredictions = convert.listTodoubleArray(predictions);

                //TODO: check which argument for matrix[][model?]

                for (int i = 0; i < doubleArrayPredictions.length; i++) {
                    matrix[idx][cnt] = doubleArrayPredictions[i];
                    idx++;
                }
            }
            cnt++;
            // get list of models trained on whole levelZeroDataset
            levelZeroModels.add(noCVbaseModel);
        }

        // levelOneDataset
        List<LabeledPoint> levelOneDataset = convert.matrixtoLabeledPoint(matrix, convert.getLabelsFolds(folds,
                (int) trainDataset.count()));

        JavaRDD<LabeledPoint> levelOneDistData = Parallelize.convertToJavaRDD(levelOneDataset);

        // Train level1 Classifier using levelOneDistData
        levelOneModel = build.buildRandomForest(levelOneDistData);


    }

    public void train(JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels) throws MLModelHandlerException,
            MLModelBuilderException {

        train(2, trainDataset, baseModels, 3, 12345);

    }

    public JavaPairRDD<Double, Double> test(long modelId, JavaRDD<LabeledPoint> testDataset)
            throws MLModelHandlerException {

        // Step1. test level0 models trained on whole dataset using level0 test
        // step2. Combine predictions from step1 into level1 testset
        //Step3. test level1 model using level1 dataset

        // Predict on levelZeroTestDataset to get levelOneTestDataset
        Util convert = new Util();
        double[][] matrix = new double[(int) testDataset.count()][levelZeroModels.size()];

        List<String[]> dataTobePredicted = convert.LabeledpointToListStringArray(testDataset);
        int cnt = 0;
        for (MLModel model : levelZeroModels) {
            int idx = 0;
            Predictor predictor = new Predictor(modelId, model, dataTobePredicted);
            List<Double> predictions = (List<Double>) predictor.predict();


            for (int i = 0; i < predictions.size(); i++) {
                matrix[idx][cnt] = predictions.get(i);
                idx++;
            }
            cnt++;
        }

        List<LabeledPoint> levelOneTestDataset = convert.matrixtoLabeledPoint(matrix, convert.getLabels(testDataset));


        JavaRDD<LabeledPoint> levelOneDistTestData = Parallelize.convertToJavaRDD(levelOneTestDataset);
        List<String[]> LevelOneTestDatasetList = convert.LabeledpointToListStringArray(levelOneDistTestData);
        Predictor predictor = new Predictor(modelId, levelOneModel, LevelOneTestDatasetList);

        List<Double> levelOnePredictions = (List<Double>) predictor.predict();
        List<Double> labelsList = Doubles.asList(convert.getLabels(testDataset));

        List<Tuple2<Double, Double>> list = new ArrayList<Tuple2<Double, Double>>();
        for (int j = 0; j < levelOnePredictions.size(); j++) {
            list.add(new Tuple2<Double, Double>(levelOnePredictions.get(j), labelsList.get(j)));

        }
        return Parallelize.parallelizeList().parallelizePairs(list);

    }

    public JavaPairRDD<Double, Double> test(JavaRDD<LabeledPoint> testDataset) throws MLModelHandlerException,
            MLModelBuilderException {

        test(2, testDataset);

        return null;
    }

    public JavaRDD<LabeledPoint> blend() {
        //TODO: CLEAN CODE: helper method for getting predictions of basemodels and combining them to get levelonedataset
        return null;
    }


}
