import com.joptimizer.functions.ConvexMultivariateRealFunction;
import com.joptimizer.functions.LinearMultivariateRealFunction;
import com.joptimizer.functions.PDQuadraticMultivariateRealFunction;
import com.joptimizer.optimizers.JOptimizer;
import com.joptimizer.optimizers.OptimizationRequest;
import org.apache.commons.math.linear.MatrixUtils;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by pekasa on 05.05.16.
 */


public class StackingWithQP {

    public static void main(String[] args) throws IOException {
        StackingWithQP s = new StackingWithQP();
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
     * This method trains an StackingWithQP ensemble model
     *
     * @param  trainDataset            Training dataset as a JavaRDD of labeled points
     * @param  baseModels              List of basemodels selected for ensembling using StackingWithQP
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
        List<Double> labels = null;

        ArrayList<ArrayList<Double>> matrix = new ArrayList<ArrayList<Double>>();

        // TODO: For every Fold train and test a model passed as list of model, combine predictions, compute alpha
        for (String model: baseModels) {
            int idx = 0;
            for (Tuple2<RDD<LabeledPoint>, RDD<LabeledPoint>> fold: folds) {
                try {
                    modelPredictions =  build.buildBaseModels(model, fold._1().toJavaRDD(),fold._2().toJavaRDD());
                    predictions = modelPredictions.keys().collect();
                    labels = modelPredictions.values().collect();
                    matrix.add((ArrayList<Double>) predictions);

                    //TODO: There must be a way to convert javapairrdd to matrix

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }

        matrix.add((ArrayList<Double>) labels);

        double[][] doubleMatrix = convertArrayListdoubleArray(matrix, (int) trainDataset.count(), baseModels.size());

        System.out.println("ARRAYLISTMATRIX" +matrix.get(0).size()+"\n \n");
        System.out.print(Arrays.deepToString(doubleMatrix));

    }
    /*
    Method to compute weights for combining predictions of basemodels
     */
    public double[] computeWeights(double[][] level1Dataset, double[] labels, int numOfModels) throws Exception {
        RealMatrix m = MatrixUtils.createRealMatrix(level1Dataset);
        RealMatrix n = m.transpose();
        RealMatrix P = m.multiply(n);

         PDQuadraticMultivariateRealFunction objectiveFunction = new PDQuadraticMultivariateRealFunction(P.getData(), null, 0);

        //equalities
        double[][] A = new double[][]{{1,1}};
        double[] b = new double[]{1};

        //inequalities
        ConvexMultivariateRealFunction[] inequalities = new ConvexMultivariateRealFunction[2];
        RealMatrix negativeIdentityMatrix = MatrixUtils.createRealIdentityMatrix(numOfModels);
        negativeIdentityMatrix = negativeIdentityMatrix.scalarMultiply(-1.0);
        negativeIdentityMatrix = negativeIdentityMatrix.scalarAdd(0.0);
        for (int i = 0; i <= numOfModels; i++) {
            inequalities[0] = new LinearMultivariateRealFunction(negativeIdentityMatrix.getData()[i], 0);

        }

        //optimization problem
        OptimizationRequest or = new OptimizationRequest();
        or.setF0(objectiveFunction);
        or.setInitialPoint(new double[] { 0.1, 0.9});
        or.setFi(inequalities); //if you want x>0 and y>0
        or.setA(A);
        or.setB(b);
        or.setToleranceFeas(1.E-12);
        or.setTolerance(1.E-12);

        //optimization
        JOptimizer opt = new JOptimizer();
        opt.setOptimizationRequest(or);
        int returnCode = opt.optimize();
        double[] weights = opt.getOptimizationResponse().getSolution();
        return weights;



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


    public void train(JavaRDD<LabeledPoint> trainDataset, ArrayList<String> baseModels){

        train(trainDataset,baseModels,2, 12345);

    }


    }


