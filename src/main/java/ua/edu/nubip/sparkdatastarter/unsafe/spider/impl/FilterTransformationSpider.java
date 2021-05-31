package ua.edu.nubip.sparkdatastarter.unsafe.spider.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import ua.edu.nubip.sparkdatastarter.unsafe.spider.TransformationSpider;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.FilterTransformation;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.SparkTransformation;
import ua.edu.nubip.sparkdatastarter.unsafe.util.WordsMatcher;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Component("findBy")
@RequiredArgsConstructor
public class FilterTransformationSpider implements TransformationSpider {

    private final Map<String, FilterTransformation> transformationMap;

    @Override
    public Tuple2<SparkTransformation, List<String>> createTransformation(List<String> remainingWords, Set<String> fieldNames) {
        String fieldName = WordsMatcher.findAndRemoveMatchingPiecesIfExists(fieldNames, remainingWords);
        String filterName = WordsMatcher.findAndRemoveMatchingPiecesIfExists(transformationMap.keySet(), remainingWords);
        return new Tuple2<>(transformationMap.get(filterName), List.of(fieldName));
    }
}
