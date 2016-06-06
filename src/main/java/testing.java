import org.wso2.carbon.ml.commons.constants.MLConstants;
import org.wso2.carbon.ml.commons.domain.MLModel;
import org.wso2.carbon.ml.core.exceptions.MLModelHandlerException;
import org.wso2.carbon.ml.core.impl.Predictor;
import org.wso2.carbon.ml.core.spark.models.MLRandomForestModel;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pekasa on 02.06.16.
 */
public class testing {
    public static void main(String[] args) throws NullPointerException{
        MLModel ml = new MLModel();
        ml.setAlgorithmName(MLConstants.SUPERVISED_ALGORITHM.RANDOM_FOREST_CLASSIFICATION.toString());
        MLRandomForestModel rf = new MLRandomForestModel();
        ml.setModel(rf);
        ml.setAlgorithmClass("Classification");

        long modelId = 2;
        List<String[]> data = new ArrayList<String[]>();
        String datapoints[] = {"1.0","0.1","1.0"};
        data.add(datapoints);
        Predictor predictor = new Predictor(modelId, ml, data);
        try {
            predictor.predict();
        } catch (MLModelHandlerException e) {
            e.printStackTrace();
        }

    }
}
