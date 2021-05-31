package ua.edu.nubip.sparkdatastarter.unsafe.holder;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ConfigurationProperties(prefix = "spark")
public class SparkDataPropsHolder {
    private String appName;
    private String packagesToScan;
    private String master;
}
