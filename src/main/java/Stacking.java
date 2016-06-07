import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.wso2.carbon.ml.commons.constants.MLConstants;
import org.wso2.carbon.ml.commons.domain.MLModel;
import org.wso2.carbon.ml.core.exceptions.MLModelBuilderException;
import org.wso2.carbon.ml.core.exceptions.MLModelHandlerException;
import org.wso2.carbon.ml.core.impl.Predictor;
import org.wso2.carbon.ml.core.interfaces.MLModelBuilder;
import org.wso2.carbon.ml.core.internal.MLModelConfigurationContext;
import org.wso2.carbon.ml.core.spark.models.MLRandomForestModel;
import scala.Tuple2;

import java.io.Externalizable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pekasa on 01.06.16.
 */
public class Stacking extends MLModelBuilder{
    public Stacking(MLModelConfigurationContext context) {
        super(context);
    }

    public MLModel build() throws MLModelBuilderException {
        return null;
    }

    public MLModel buildBaseModels(JavaRDD<LabeledPoint> CvTrainingData) throws MLModelBuilderException {
        ReadCSV buildbasemodel = new ReadCSV();
        MLModelConfigurationContext context = getContext();
        MLModel ml = new MLModel();
        ml.setAlgorithmName(MLConstants.SUPERVISED_ALGORITHM.RANDOM_FOREST_CLASSIFICATION.toString());
        ml.setEncodings(context.getEncodings());
        ml.setAlgorithmClass("Classification");
        MLRandomForestModel mlRandomForest  = new MLRandomForestModel();
        RandomForestModel randomForestModel = buildbasemodel.buildRandomForest(CvTrainingData);
        mlRandomForest.setModel(randomForestModel);
        ml.setModel((Externalizable) mlRandomForest);

        return ml;
    }

    public static void main(String[] args) throws IOException, MLModelHandlerException, MLModelBuilderException {
        MLModelConfigurationContext context = null;
        Stacking s = new Stacking(context);
        Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>> datasets = s.preProcess();

       JavaRDD<LabeledPoint> training_data = datasets._1();
       JavaRDD<LabeledPoint> testing_data = datasets._2();
        ArrayList<String> models = new ArrayList<String>(2);
        models.add("DECISION_TREE");
        models.add("RANDOM_FOREST_CLASSIFICATION");

        long modelId = 2;
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
     * This method trains an StackingWithQP ensemble model
     *
     * @param  trainDataset            Training dataset as a JavaRDD of labeled points
     * @param  baseModels              List of basemodels selected for ensembling using StackingWithQP
     * @param  numFolds
     * @param  seed
     * @return
     */

    public void train( long modelId,JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels, Integer numFolds, Integer seed) throws NullPointerException, MLModelHandlerException, MLModelBuilderException {


        RDD<LabeledPoint> r = trainDataset.rdd();
        //ReadCSV build = new ReadCSV();
        ReadCSV convert = new ReadCSV();


        Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>>[] folds =  MLUtils.kFold(r, numFolds, seed, trainDataset.classTag());
        JavaPairRDD<Double, Double> modelPredictions = null;


        ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();

        // TODO: For every Fold train and test a model passed as list of model, combine predictions, compute alpha
        for (String model: baseModels) {
             for (Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>> fold: folds) {

                 //modelPredictions =  build.buildBaseModels(model, fold._1().toJavaRDD(),fold._2().toJavaRDD());
                 JavaRDD<LabeledPoint> validationData = fold._2().toJavaRDD();
                 List<String[]> dataTobePredicted = convert.LabeledpointToListStringArray(validationData);
                 System.out.print(dataTobePredicted);
                 MLModel basemodel = buildBaseModels(fold._1.toJavaRDD());


                 Predictor predictor = new Predictor(modelId, basemodel,dataTobePredicted );
                 List<?> predictions = predictor.predict();
                 System.out.println(predictions);


                 //TODO: There must be a way to convert javapairrdd to matrix

             }


        }


        //double[][] doubleMatrix = convertArrayListdoubleArray(matrix, (int) trainDataset.count(), baseModels.size());


    }

    public void train( JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels) throws MLModelHandlerException, MLModelBuilderException {

        train(2, trainDataset,baseModels,2, 12345);

    }


}
