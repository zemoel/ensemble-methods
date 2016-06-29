import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.List;

/**
 * Created by pekasa on 29.06.16.
 */
public class Bagging {

    public void train(JavaSparkContext sparkContext, long modelId, JavaRDD<LabeledPoint> trainData,
                      List<String> baseModels){


        for(int i= 0; i< baseModels.size(); i++){

            // randomly sample data with replacement
            // train classifier on each bootstrap sample

        }

    }
}
