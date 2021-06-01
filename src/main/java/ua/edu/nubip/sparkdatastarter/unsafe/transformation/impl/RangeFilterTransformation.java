package ua.edu.nubip.sparkdatastarter.unsafe.transformation.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;
import ua.edu.nubip.sparkdatastarter.unsafe.collection.OrderedBag;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.FilterTransformation;

import java.util.List;

@Component("range")
public class RangeFilterTransformation implements FilterTransformation {
    @Override
    public Dataset<Row> transform(Dataset<Row> dataset, List<String> fieldNames, OrderedBag<Object> args) {
        return dataset
                .filter(functions.to_date(functions.col(fieldNames.get(0)), "dd.MM.yyyy")
                        .between(functions.lit(args.takeAndRemove()), functions.lit(args.takeAndRemove())));
    }
}
