/**
 * Created by pekasa on 05.05.16.
 */
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ReadCSV {
    public static void readCSVFile(String fileName){
        File csvData = new File("/path/to/csv");
        CSVParser parser = CSVParser.parse(csvData, CSVFormat.RFC4180);
        for (CSVRecord csvRecord : parser) {
            ...
        }

    }
}
