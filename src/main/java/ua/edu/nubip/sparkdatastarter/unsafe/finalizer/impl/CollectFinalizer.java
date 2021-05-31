package ua.edu.nubip.sparkdatastarter.unsafe.finalizer.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;
import ua.edu.nubip.sparkdatastarter.unsafe.collection.OrderedBag;
import ua.edu.nubip.sparkdatastarter.unsafe.finalizer.Finalizer;

@Component("collect")
public class CollectFinalizer implements Finalizer {
    @Override
    public Object doAction(Dataset<Row> dataset, Class<?> modelClass, OrderedBag<?> args) {
        Encoder<?> encoder = Encoders.bean(modelClass);
//        List<String> listFieldNames = Arrays.stream(encoder.schema().fields()).filter(structField -> structField.dataType() instanceof ArrayType)
//                .map(StructField::name)
//                .collect(Collectors.toList());
//        for (String fieldName : listFieldNames) {
//            ParameterizedType genericType = (ParameterizedType) modelClass.getDeclaredField(fieldName).getGenericType();
//            Class c = (Class) genericType.getActualTypeArguments()[0];
//            dataset.withColumn(fieldName, functions.lit(null).cast(DataTypes.createStructType(Encoders.bean(c).schema().fields())));
//        }
        return dataset.as(encoder).collectAsList();    }
}
