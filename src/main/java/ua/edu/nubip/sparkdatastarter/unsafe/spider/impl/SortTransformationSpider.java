package ua.edu.nubip.sparkdatastarter.unsafe.spider.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import ua.edu.nubip.sparkdatastarter.unsafe.spider.TransformationSpider;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.SparkTransformation;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.impl.OrderByTransformation;
import ua.edu.nubip.sparkdatastarter.unsafe.util.WordsMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Component("orderBy")
@RequiredArgsConstructor
public class SortTransformationSpider implements TransformationSpider {

    private final OrderByTransformation orderByTransformation;

    @Override
    public Tuple2<SparkTransformation, List<String>> createTransformation(List<String> remainingWords, Set<String> fieldNames) {
        String fieldName = WordsMatcher.findAndRemoveMatchingPiecesIfExists(fieldNames, remainingWords);
        List<String> additionalFields = new ArrayList<>();
        while (!remainingWords.isEmpty() && remainingWords.get(0).equalsIgnoreCase("and")) {
            remainingWords.remove(0);
            additionalFields.add(WordsMatcher.findAndRemoveMatchingPiecesIfExists(fieldNames, remainingWords));
        }
        additionalFields.add(0, fieldName);
        return new Tuple2<>(orderByTransformation, additionalFields);
    }
}
