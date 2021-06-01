package ua.edu.nubip.sparkdatastarter.unsafe.finalizer.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;
import ua.edu.nubip.sparkdatastarter.unsafe.collection.OrderedBag;
import ua.edu.nubip.sparkdatastarter.unsafe.finalizer.Finalizer;

@Component("count")
public class CountFinalizer implements Finalizer {
    @Override
    public Object doAction(Dataset<Row> dataset, Class<?> modelClass, OrderedBag<?> args) {
        return dataset.count();
    }
}
