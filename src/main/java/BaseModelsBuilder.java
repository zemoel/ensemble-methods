import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.wso2.carbon.ml.commons.constants.MLConstants;
import org.wso2.carbon.ml.commons.domain.MLModel;
import org.wso2.carbon.ml.core.exceptions.MLModelBuilderException;
import org.wso2.carbon.ml.core.spark.algorithms.DecisionTree;
import org.wso2.carbon.ml.core.spark.algorithms.RandomForestClassifier;
import org.wso2.carbon.ml.core.spark.models.MLDecisionTreeModel;
import org.wso2.carbon.ml.core.spark.models.MLRandomForestModel;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by pekasa on 07.06.16.
 */
public class BaseModelsBuilder  {

    /**

     create a switch statement for methods which call sparkmllibrary libraries and return models of MLModel type
      */


    public Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>> preProcess() throws IOException {
        ReadCSV read = new ReadCSV();
        JavaRDD<LabeledPoint> inputData = read.readCSV();
        JavaRDD<LabeledPoint>[] tmp = inputData.randomSplit(new double[]{0.7, 0.3});

        return new Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>>(tmp[0], tmp[1]);
    }



    public MLModel buildBaseModels(String algorithmName, JavaRDD<LabeledPoint> cvTrainingData, JavaRDD<LabeledPoint> validationData) throws MLModelBuilderException {

        MLConstants.SUPERVISED_ALGORITHM supervisedAlgorithm = MLConstants.SUPERVISED_ALGORITHM.valueOf(algorithmName);
        MLModel model = null;


        switch(supervisedAlgorithm){
            case RANDOM_FOREST_CLASSIFICATION:
                 model = buildRandomForest(cvTrainingData);
                break;
            case DECISION_TREE:
                 model =  buildDecisionTree(cvTrainingData, validationData);
                break;

        }


        return model;
    }
    public MLModel buildRandomForest(JavaRDD<LabeledPoint> trainingData){

        MLModel model = new MLModel();
        List<Map<String, Integer>> encodings = new ArrayList<Map<String, Integer>>();
        Map<String, Integer> mappings = new HashMap<String, Integer>();
        encodings.add(mappings);
        model.setAlgorithmName(MLConstants.SUPERVISED_ALGORITHM.RANDOM_FOREST_CLASSIFICATION.toString());
        model.setEncodings(encodings);
        model.setAlgorithmClass("Classification");

        RandomForestClassifier randomForestModel = new RandomForestClassifier();
        Integer numClasses = 4;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        Integer numTrees = 3; // Use more in practice.
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "entropy";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

        final RandomForestModel randomForestmodel = randomForestModel.train(trainingData, numClasses,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                seed);

        // remove from cache
        trainingData.unpersist();
        // add test data to cache
        // validationData.cache();
        MLRandomForestModel mlRandomForest  = new MLRandomForestModel();
        mlRandomForest.setModel(randomForestmodel);
        model.setModel(mlRandomForest);



        return model;
    }


    public MLModel buildDecisionTree(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> validationData){

        MLModel model = new MLModel();
        List<Map<String, Integer>> encodings = new ArrayList<Map<String, Integer>>();
        Map<String, Integer> mappings = new HashMap<String, Integer>();
        encodings.add(mappings);
        model.setAlgorithmName(MLConstants.SUPERVISED_ALGORITHM.DECISION_TREE.toString());
        model.setEncodings(encodings);
        model.setAlgorithmClass("Classification");

        Integer numClasses = 4;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        Integer maxDepth = 5;
        Integer maxBins = 32;
        String impurity = "entropy";

        DecisionTree decisionTree = new DecisionTree();
        DecisionTreeModel decisionTreeModel = decisionTree.train(trainingData, numClasses,
                categoricalFeaturesInfo,impurity,
                maxDepth,
                maxBins);
        trainingData.unpersist();
        // add test data to cache
        validationData.cache();

        MLDecisionTreeModel mlDecisionTree  = new MLDecisionTreeModel();
        mlDecisionTree.setModel(decisionTreeModel);
        model.setModel(mlDecisionTree);

        return model;
    }
}
