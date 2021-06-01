package ua.edu.nubip.sparkdatastarter.unsafe.transformation.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;
import ua.edu.nubip.sparkdatastarter.unsafe.collection.OrderedBag;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.FilterTransformation;

import java.util.List;

@Component("equals")
public class EqualsFilterTransformation implements FilterTransformation {
    @Override
    public Dataset<Row> transform(Dataset<Row> dataset, List<String> fieldNames, OrderedBag<Object> args) {
        return dataset.filter(functions.col(fieldNames.get(0)).equalTo(args.takeAndRemove()));
    }
}
