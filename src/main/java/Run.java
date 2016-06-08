import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.wso2.carbon.ml.core.exceptions.MLModelBuilderException;
import org.wso2.carbon.ml.core.exceptions.MLModelHandlerException;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by pekasa on 08.06.16.
 */
public class Run {
    public static void main(String[] args) throws IOException, MLModelHandlerException, MLModelBuilderException {


        Stacking s =  new Stacking();
        ReadCSV parse = new ReadCSV();


        Tuple2<JavaRDD<LabeledPoint>, JavaRDD<LabeledPoint>> datasets = parse.preProcess();

        JavaRDD<LabeledPoint> training_data = datasets._1();
        JavaRDD<LabeledPoint> testing_data = datasets._2();
        ArrayList<String> models = new ArrayList<String>(2);
        models.add("DECISION_TREE");
        models.add("RANDOM_FOREST_CLASSIFICATION");
        s.train(training_data, models);

        //summary = s.test(testing_data, model);

    }
}
