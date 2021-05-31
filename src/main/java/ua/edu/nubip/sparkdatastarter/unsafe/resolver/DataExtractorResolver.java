package ua.edu.nubip.sparkdatastarter.unsafe.resolver;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ua.edu.nubip.sparkdatastarter.unsafe.extractor.DataExtractor;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class DataExtractorResolver {

    private final Map<String, DataExtractor> extractorMap;

    public DataExtractor resolve(String pathToDate) {
        String fileWExt = pathToDate.split("\\.")[1];
        return extractorMap.get(fileWExt);
    }
}
