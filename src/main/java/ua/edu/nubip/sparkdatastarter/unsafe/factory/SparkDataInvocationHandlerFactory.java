package ua.edu.nubip.sparkdatastarter.unsafe.factory;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import ua.edu.nubip.sparkdatastarter.unsafe.annotation.Source;
import ua.edu.nubip.sparkdatastarter.unsafe.annotation.Transient;
import ua.edu.nubip.sparkdatastarter.unsafe.extractor.DataExtractor;
import ua.edu.nubip.sparkdatastarter.unsafe.finalizer.Finalizer;
import ua.edu.nubip.sparkdatastarter.unsafe.invocation.SparkDataInvocationHandler;
import ua.edu.nubip.sparkdatastarter.unsafe.invocation.impl.SparkDataInvocationHandlerImpl;
import ua.edu.nubip.sparkdatastarter.unsafe.repository.SparkRepository;
import ua.edu.nubip.sparkdatastarter.unsafe.resolver.DataExtractorResolver;
import ua.edu.nubip.sparkdatastarter.unsafe.spider.TransformationSpider;
import ua.edu.nubip.sparkdatastarter.unsafe.transformation.SparkTransformation;
import ua.edu.nubip.sparkdatastarter.unsafe.util.WordsMatcher;

import java.beans.Introspector;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

@Component
@RequiredArgsConstructor
public class SparkDataInvocationHandlerFactory {

    private final DataExtractorResolver extractorResolver;
    private final Map<String, TransformationSpider> spiderMap;
    private final Map<String, Finalizer> finalizerMap;

    @Setter
    private ConfigurableApplicationContext realContext;

    public SparkDataInvocationHandler create(Class<? extends SparkRepository> repositoryInterface) {
        Class<?> modelClass = getModelClass(repositoryInterface);
        Set<String> fieldNames = getFieldNames(modelClass);
        String pathToDate = modelClass.getAnnotation(Source.class).value();
        DataExtractor dataExtractor = extractorResolver.resolve(pathToDate);

        Map<Method, List<Tuple2<SparkTransformation, List<String>>>> transformationChaim = new HashMap<>();
        Map<Method, Finalizer> method2finalizer = new HashMap<>();

        Method[] methods = repositoryInterface.getMethods();

        for (Method method : methods) {
            TransformationSpider currentSpider = null;
            List<Tuple2<SparkTransformation, List<String>>> transformations = new ArrayList<>();
            List<String> methodWords = new ArrayList<>(asList(method.getName().split("(?=\\p{Upper})")));
            while (methodWords.size() > 1) {
                String strategyName = WordsMatcher.findAndRemoveMatchingPiecesIfExists(spiderMap.keySet(), methodWords);
                if (!strategyName.isEmpty()) {
                    currentSpider = spiderMap.get(strategyName);
                }
                transformations.add(currentSpider.createTransformation(methodWords, fieldNames));
            }
            String finalizerName = "collect";
            if (methodWords.size() == 1) {
                finalizerName = Introspector.decapitalize(methodWords.get(0));
            }
            transformationChaim.put(method, transformations);
            method2finalizer.put(method, finalizerMap.get(finalizerName));
        }

        return SparkDataInvocationHandlerImpl.builder()
                .modelClass(modelClass)
                .pathToData(pathToDate)
                .finalizerMap(method2finalizer)
                .transformationChaim(transformationChaim)
                .dataExtractor(dataExtractor)
                .context(realContext)
                .build();
    }

    private Class<?> getModelClass(Class<? extends SparkRepository> repositoryInterface) {
        ParameterizedType genericInterface = (ParameterizedType) repositoryInterface.getGenericInterfaces()[0];
        Class<?> modelClass = (Class<?>) genericInterface.getActualTypeArguments()[0];
        return modelClass;
    }

    private Set<String> getFieldNames(Class<?> modelClass) {
        return Arrays.stream(modelClass.getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Collection.class.isAssignableFrom(field.getType()))
                .map(Field::getName)
                .collect(Collectors.toSet());
    }
}
