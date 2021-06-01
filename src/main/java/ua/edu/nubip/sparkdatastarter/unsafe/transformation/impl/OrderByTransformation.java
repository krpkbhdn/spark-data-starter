package ua.edu.nubip.sparkdatastarter.unsafe.transformation.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;
import ua.edu.nubip.sparkdatastarter.unsafe.collection.OrderedBag;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.SortTransformation;

import java.util.List;

@Component
public class OrderByTransformation implements SortTransformation {
    @Override
    public Dataset<Row> transform(Dataset<Row> dataset, List<String> fieldNames, OrderedBag<Object> args) {
        return dataset.orderBy(fieldNames.get(0), fieldNames.stream().skip(1).toArray(String[]::new));
    }
}
