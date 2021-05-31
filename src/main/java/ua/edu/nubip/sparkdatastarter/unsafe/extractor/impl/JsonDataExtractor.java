package ua.edu.nubip.sparkdatastarter.unsafe.extractor.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import ua.edu.nubip.sparkdatastarter.unsafe.extractor.DataExtractor;

@Component("json")
public class JsonDataExtractor implements DataExtractor {
    @Override
    public Dataset<Row> readData(String pathToData, ConfigurableApplicationContext context) {
        return context.getBean(SparkSession.class).read().json(pathToData);
    }
}
