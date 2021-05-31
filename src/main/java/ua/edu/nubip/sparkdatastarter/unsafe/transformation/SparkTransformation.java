package ua.edu.nubip.sparkdatastarter.unsafe.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ua.edu.nubip.sparkdatastarter.unsafe.collection.OrderedBag;

import java.util.List;

public interface SparkTransformation {
    Dataset<Row> transform(Dataset<Row> dataset, List<String> fieldNames, OrderedBag<Object> args);
}
