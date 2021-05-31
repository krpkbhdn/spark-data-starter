package ua.edu.nubip.sparkdatastarter.unsafe.spider;

import scala.Tuple2;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.SparkTransformation;

import java.util.List;
import java.util.Set;

public interface TransformationSpider {
    Tuple2<SparkTransformation, List<String>> createTransformation(List<String> remainingWords, Set<String> fieldNames);
}
