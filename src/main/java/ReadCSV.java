/**
 * Created by pekasa on 05.05.16.
 */


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.wso2.carbon.ml.commons.constants.MLConstants;
import org.wso2.carbon.ml.core.spark.algorithms.DecisionTree;
import org.wso2.carbon.ml.core.spark.algorithms.RandomForestClassifier;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ReadCSV{
     //parse a csv file and convert it to JavaRDD
    /*
    public ReadCSV(MLModelConfigurationContext context) {
         super(context);
     }
    public MLModel build() throws MLModelBuilderException {
        MLModelConfigurationContext context = getContext();
        JavaSparkContext sparkContext = null;
        DatabaseService databaseService = MLCoreServiceValueHolder.getInstance().getDatabaseService();
        MLModel mlModel = new MLModel();
        try {
            sparkContext = context.getSparkContext();
            Workflow workflow = context.getFacts();
            long modelId = context.getModelId();

            // Verify validity of response variable
            String typeOfResponseVariable = getTypeOfResponseVariable(workflow.getResponseVariable(),
                    workflow.getFeatures());

            if (typeOfResponseVariable == null) {
                throw new MLModelBuilderException("Type of response variable cannot be null for supervised learning "
                        + "algorithms.");
            }

            // Stops model building if a categorical attribute is used with numerical prediction
            if (workflow.getAlgorithmClass().equals(AlgorithmType.NUMERICAL_PREDICTION.getValue())
                    && typeOfResponseVariable.equals(FeatureType.CATEGORICAL)) {
                throw new MLModelBuilderException("Categorical attribute " + workflow.getResponseVariable()
                        + " cannot be used as the response variable of the Numerical Prediction algorithm: "
                        + workflow.getAlgorithmName());
            }
        } catch (DatabaseHandlerException e) {
            throw new MLModelBuilderException("An error occurred while building supervised machine learning model: "
                    + e.getMessage(), e);
        }
        return null;
    }
    public MLModel getBaseModels(String algorithmName){
        MLModel baseModels = new MLModel();
        baseModels.setAlgorithmName(MLConstants.SUPERVISED_ALGORITHM.valueOf(algorithmName).toString());
        MLModelConfigurationContext context = getContext();
        sparkContext = context.getSparkContext();
        Workflow workflow = context.getFacts();
        SupervisedSparkModelBuilder build = new SupervisedSparkModelBuilder();

        return baseModels;
    }
*/
    public static void main(String[] args) throws IOException {
        //ReadCSV.buildBaseModels();
        ReadCSV build = new ReadCSV();
        //build.buildBaseModels();
       JavaRDD<LabeledPoint> rddata = build.readCSV();
       // build.parse();


    }
    public JavaSparkContext convertToRDD(){
        SparkConf conf = new SparkConf().setAppName("Ensemble").setMaster("local[2]");//.set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;

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
         //System.out.println(labeledList);

        return  distData;


    }




    public JavaPairRDD<Double, Double> buildBaseModels(String algorithmName, JavaRDD<LabeledPoint> training_data,
                                                         JavaRDD<LabeledPoint> validation_data) throws IOException {
        // Method for test how RandomForest works on iris dataset.
        //TODO: Add another basemodel, combine predictions in a JavaPairRDD[]<LabeledPoint>




        // Creating switch statement
        MLConstants.SUPERVISED_ALGORITHM supervisedAlgorithm = MLConstants.SUPERVISED_ALGORITHM.valueOf(algorithmName);

       RandomForestModel model;
        DecisionTreeModel decisionTreeModel;

        switch(supervisedAlgorithm){
            case RANDOM_FOREST_CLASSIFICATION:
                 model= buildRandomForest(training_data);
                break;
            case DECISION_TREE:
                decisionTreeModel =  buildDecisionTree(training_data, validation_data);
            break;

        }
        return null;

    }
    public RandomForestModel buildRandomForest(JavaRDD<LabeledPoint> trainingData){

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

            //JavaPairRDD<Double, Double> predictionsAndLabels = randomForestModel.test(model, validationData).cache();
            //System.out.println(predictionsAndLabels.values());

            return randomForestmodel;
        }

    public  DecisionTreeModel buildDecisionTree(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> validationData){

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

        //JavaPairRDD<Double, Double> predictionsAndLabels = decisionTree.test(decisionTreeModel, validationData)
              //  .cache();

        return decisionTreeModel;
    }


}


