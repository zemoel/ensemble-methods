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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pekasa on 01.06.16.
 */
public class Stacking {

    private MLModel levelOneModel;
    private List<MLModel> levelZeroModels;
    /**
     * This method trains an ComputeWeightsQPSt ensemble model
     *
     * @param  trainDataset            Training dataset as a JavaRDD of labeled points
     * @param  baseModels              List of basemodels selected for ensembling using ComputeWeightsQPSt
     * @param  numFolds
     * @param  seed
     * @return
     */

    public void train( long modelId,JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels, Integer numFolds, Integer seed) throws NullPointerException, MLModelHandlerException, MLModelBuilderException {

        // Step1. train list of level0models on cvdata
        // Step2. get predictions of each List<?> and combine predictions to get level1 dataset
        // Step3. train level1model on level1 dataset
        // Step4. train level0models on whole dataset and store list of models

        // get list of basemodels, iterate over them, pass them  one by one as algorithm name to basemodelbuilder
        // get the model and invoke predictor.
        // combine predictions to list of list see supuns idea
        // See the Steps above.


        RDD<LabeledPoint> r = trainDataset.rdd();

        ReadCSV convert = new ReadCSV();
        BaseModelsBuilder build =  new BaseModelsBuilder();


        Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>>[] folds =  MLUtils.kFold(r, numFolds, seed, trainDataset.classTag());
        List<LabeledPoint> labeledList = new ArrayList<LabeledPoint>();
        ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();

        // TODO: For every Fold train and test a model passed as list of model, combine predictions, compute alpha
        for (String model: baseModels) {
             for (Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>> fold: folds) {

                 JavaRDD<LabeledPoint> validationData = fold._2().toJavaRDD();
                 List<String[]> dataTobePredicted = convert.LabeledpointToListStringArray(validationData);
                 MLModel basemodel = build.buildBaseModels(model, fold._1.toJavaRDD(), fold._2.toJavaRDD());
                 Predictor predictor = new Predictor(modelId, basemodel,dataTobePredicted );
                 List<?> predictions = predictor.predict();

                 System.out.println("RandomForestPredictions" +predictions);


                 //TODO: There must be a way to convert javapairrdd to matrix

             }
        }


        // add predictions here to a list to form level1dataset
        // Call RandomForest on level1 dataset and set it to levelOneModel
        levelOneModel = null;
        levelZeroModels = null;


    }

    public void train( JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels) throws MLModelHandlerException, MLModelBuilderException {

        train(2, trainDataset,baseModels,2, 12345);

    }

    public JavaPairRDD<Double, Double>  test(JavaRDD<LabeledPoint> testData){

        // Step1. test level0 models trained on whole dataset using level0 test
        // step2. Combine predictions from step1 into level1 testset
        //Step3. test level1 model using level1 dataset
        // JavapairRDD<DOuble, Double>

        return null;
    }


}
