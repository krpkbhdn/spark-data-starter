package ua.edu.nubip.sparkdatastarter.unsafe.finalizer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ua.edu.nubip.sparkdatastarter.unsafe.collection.OrderedBag;

public interface Finalizer {
    Object doAction(Dataset<Row> dataset, Class<?> modelClass, OrderedBag<?> args);
}
