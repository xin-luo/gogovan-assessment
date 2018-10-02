package ggv.utilities;

import com.amazonaws.SDKGlobalConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Arrays;

/**
 * Attempts to runs Kinesalite and Dynalite locally if setting is enabled.
 */
@Slf4j
@Service
public class LocalKinesisExecutor {
    private final ConfigurationProvider configuration;
    private final KinesisUtilities utilities;
    public LocalKinesisExecutor(ConfigurationProvider configuration, KinesisUtilities utilities) throws IOException {
        this.configuration = configuration;
        this.utilities = utilities;

        if (configuration.isAwsIsLocal()) {
            //If running local Kinesis, need to disable some configurations that do not apply

            //disable cbor
            System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");

            //dont check certs, this option a bit flaky, best to set it in vm options directly "-Dcom.amazonaws.sdk.disableCertChecking"
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");

            //use dummy access/secret keys
            System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR, "01234567890123456789");
            System.setProperty(SDKGlobalConfiguration.SECRET_KEY_ENV_VAR, "0123456789012345678901234567890123456789");

            //use ssl instead of tls
            System.setProperty("USE_SSL", "true");

            //Run Processes. Note that if your app gets in a bad state it won't be able to clean up after itself and
            //there might be rogue node executables still running afterwards. You may need to kill them manually before
            //restarting the app
            Runtime runtime = Runtime.getRuntime();
            String shell = (System.getProperty("os.name").toLowerCase().startsWith("windows")) ? "cmd" : "sh";

            int kinesisPort = configuration.getKinesisPort();
            Process kinesaliteProcess = runtime.exec(String.format("%s /c kinesalite --ssl --port %d", shell, kinesisPort));
            log.info("Spun up local kinesalite process on port {}", kinesisPort);

            int dynamoPort = configuration.getDynamodbPort();
            Process dyanliteProcess = runtime.exec(String.format("%s /c dynalite --ssl --port %d", shell, dynamoPort));
            log.info("Spun up local dynalite process on port {}", dynamoPort);
        }
    }

    @PostConstruct
    public void init() {
        if (configuration.isAwsIsLocal()) {
            //creates new streams for each order type in parallel
            Arrays.asList(configuration.getOrderCreatedStreamName(),
                    configuration.getOrderAssignedStreamName(),
                    configuration.getOrderCompletedStreamName(),
                    configuration.getOrderCancelledStreamName())
                    .parallelStream().forEach((streamName) -> {
                try {
                    log.info("Creating stream {}...", streamName);
                    utilities.waitForStreamActive(streamName, true);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    log.error("Interrupted while attempting to create stream {}", streamName);
                }
            });
        }
    }
}