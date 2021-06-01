package ua.edu.nubip.sparkdatastarter.unsafe;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.reflections.Reflections;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ua.edu.nubip.sparkdatastarter.unsafe.config.InternalConfiguration;
import ua.edu.nubip.sparkdatastarter.unsafe.factory.SparkDataInvocationHandlerFactory;
import ua.edu.nubip.sparkdatastarter.unsafe.repository.SparkRepository;

import java.beans.Introspector;
import java.lang.reflect.Proxy;

public class SparkDataApplicationContextInitializer implements ApplicationContextInitializer {
    @Override
    public void initialize(ConfigurableApplicationContext context) {
        AnnotationConfigApplicationContext tempContext = new AnnotationConfigApplicationContext(InternalConfiguration.class);
        SparkDataInvocationHandlerFactory factory = tempContext.getBean(SparkDataInvocationHandlerFactory.class);
        factory.setRealContext(context);
        tempContext.close();

        registerSparkBeans(context);
        Reflections scanner = new Reflections(context.getEnvironment().getProperty("spark.packages-to-scan"));
        scanner.getSubTypesOf(SparkRepository.class).forEach(sparkRepositoryInterface -> {
            Object crang = Proxy.newProxyInstance(sparkRepositoryInterface.getClassLoader()
                    , new Class[]{sparkRepositoryInterface}
                    , factory.create(sparkRepositoryInterface));
            context.getBeanFactory().registerSingleton(Introspector.decapitalize(sparkRepositoryInterface.getSimpleName()), crang);
        });
    }

    private void registerSparkBeans(ConfigurableApplicationContext context) {
        SparkSession sparkSession = SparkSession.builder()
                .master(context.getEnvironment().getProperty("spark.master"))
                .appName(context.getEnvironment().getProperty("spark.app-name"))
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        context.getBeanFactory().registerSingleton("sparkSession", sparkSession);
        context.getBeanFactory().registerSingleton("sparkContext", sc);
    }
}
