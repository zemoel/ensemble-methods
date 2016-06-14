import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.wso2.carbon.ml.commons.constants.MLConstants;
import org.wso2.carbon.ml.core.exceptions.MLModelBuilderException;
import org.wso2.carbon.ml.core.exceptions.MLModelHandlerException;
import org.wso2.carbon.ml.core.spark.algorithms.Stacking;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by pekasa on 08.06.16.
 */
public class Run {
    public static void main(String[] args) throws IOException, MLModelHandlerException, MLModelBuilderException {

        ReadCSV parse = new ReadCSV();
        Stacking stacking =  new Stacking();
        Util util = new Util();
        JavaSparkContext sparkContext = Parallelize.getSparkContext();


        Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>> dataSets = parse.preProcess();


        JavaRDD<LabeledPoint> trainingData = dataSets._1();
        JavaRDD<LabeledPoint> testingData = dataSets._2();

        List<String> models = new ArrayList<String>(2);
        models.add("DECISION_TREE");
        models.add("RANDOM_FOREST_CLASSIFICATION");
        stacking.train(sparkContext,2,trainingData,models,getParameters(), MLConstants.SUPERVISED_ALGORITHM.DECISION_TREE.toString(),
                getParameters().get(0), 2, 123);
        JavaPairRDD<Double, Double> predictionsAndLabels = stacking.test(sparkContext,2,testingData);
        MulticlassMetrics multiclassMetrics = util.getMulticlassMetrics(sparkContext, predictionsAndLabels);
        Double modelAccuracy = util.getModelAccuracy(multiclassMetrics);
        System.out.println("MODEL_ACCURACY: " + modelAccuracy);

    }
    public static List<Map<String, String>> getParameters (){
        Map<String, String>  parameters = new HashMap<String, String>();
        parameters.put(MLConstants.IMPURITY , "entropy");
        parameters.put(MLConstants.MAX_DEPTH,"5" );
        parameters.put(MLConstants.MAX_BINS, "32" );
        parameters.put(MLConstants.NUM_CLASSES, "3");
        parameters.put(MLConstants.NUM_TREES, "3");
        parameters.put(MLConstants.FEATURE_SUBSET_STRATEGY, "auto");
        parameters.put(MLConstants.SEED, "12345");
        List<Map<String, String>> parametersList = new ArrayList<Map<String, String>>();
        parametersList.add(parameters);
        parametersList.add(parameters);


        return parametersList ;
    }
}
