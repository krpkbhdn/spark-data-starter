package ua.edu.nubip.sparkdatastarter.unsafe.invocation.impl;

import lombok.Builder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.context.ConfigurableApplicationContext;
import scala.Tuple2;
import ua.edu.nubip.sparkdatastarter.unsafe.collection.OrderedBag;
import ua.edu.nubip.sparkdatastarter.unsafe.extractor.DataExtractor;
import ua.edu.nubip.sparkdatastarter.unsafe.finalizer.Finalizer;
import ua.edu.nubip.sparkdatastarter.unsafe.invocation.SparkDataInvocationHandler;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.SparkTransformation;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

@Builder
public class SparkDataInvocationHandlerImpl implements SparkDataInvocationHandler {
    private Class<?> modelClass;
    private String pathToData;
    private DataExtractor dataExtractor;
    private Map<Method, List<Tuple2<SparkTransformation, List<String>>>> transformationChaim;
    private Map<Method, Finalizer> finalizerMap;

    private ConfigurableApplicationContext context;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        OrderedBag<Object> orderedBag = new OrderedBag<>(args);
        Dataset<Row> dataset = dataExtractor.readData(pathToData, context);
        List<Tuple2<SparkTransformation, List<String>>> tuple2List = transformationChaim.get(method);

        for (Tuple2<SparkTransformation, List<String>> tuple : tuple2List) {
            SparkTransformation sparkTransformation = tuple._1();
            List<String> fieldNames = tuple._2();
            dataset = sparkTransformation.transform(dataset, fieldNames, orderedBag);
        }

        Finalizer finalizer = finalizerMap.get(method);
        Object relVal = finalizer.doAction(dataset, modelClass, orderedBag);
        return relVal;
    }
}
