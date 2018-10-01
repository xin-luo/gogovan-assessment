package ggv.utilities;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.UUID;

/**
 * Shared ggv.utilities class for interacting with Kinesis
 */
@Slf4j
@Service
public class KinesisUtilities {
    private final String awsRegion;
    private final String metricsLevel;

    private final String kinesisEndpoint;
    private final int kinesisPort;
    private final int kinesisShardCount;

    private final String dynamoEndpoint;
    private final int dynamoPort;

    private AmazonDynamoDB dynamoClient;
    private AmazonKinesis kinesisClient;
    private final IMetricsFactory metricsFactory;

    private static final String STREAM_READY_STATE = "ACTIVE";

    public KinesisUtilities(ConfigurationProvider configuration, IMetricsFactory metricsFactory) {
        awsRegion = configuration.getAwsRegion();
        metricsLevel = configuration.getMetricsLevel();

        kinesisEndpoint = configuration.getKinesisEndpoint();
        kinesisPort = configuration.getKinesisPort();
        kinesisShardCount = configuration.getShardCount();

        dynamoEndpoint = configuration.getDynamodbEndpoint();
        dynamoPort = configuration.getDynamodbPort();

        this.metricsFactory = metricsFactory;
    }

    @PostConstruct
    public void init() {
        dynamoClient = getDynamoClient(dynamoEndpoint, dynamoPort, awsRegion);
        kinesisClient = getKinesisClient(kinesisEndpoint, kinesisPort, awsRegion);
    }

    /**
     * Create a single DynamoClient for use with the Kinesis Worker as Dynamo is used to save some metadata on the status
     * of the consumers (checkpointing, etc.)
     * @return
     */
    private AmazonDynamoDB getDynamoClient(String dynamodbEndpoint, int dynamodbPort, String awsRegion) {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(String.format("https://%s:%s", dynamodbEndpoint, dynamodbPort), awsRegion);
        return AmazonDynamoDBClient.builder().withEndpointConfiguration(endpointConfiguration).build();
    }

    /**
     * Get a new Kinesis Client. This is the "raw" client rather than the higher level Consumer/Producer APIs. We are
     * only using this to create a new stream if it does not exist already.
     * @return
     */
    private AmazonKinesis getKinesisClient(String kinesisEndpoint, int kinesisPort, String awsRegion) {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(String.format("https://%s:%s", kinesisEndpoint, kinesisPort), awsRegion);
        return AmazonKinesisClientBuilder.standard().withEndpointConfiguration(endpointConfiguration).build();
    }

    /**
     * When producing to or consuming from a stream that doesnt exist, an error would occur.
     * This can forcibly creates a new stream to prevent that situation. Should disable this option in production.
     * @param streamName
     * @throws InterruptedException
     */
    public void waitForStreamActive(String streamName, boolean forceCreate) throws InterruptedException {
        while (true) {
            try {
                //block while stream is not ready
                String streamStatus = kinesisClient.describeStream(streamName).getStreamDescription().getStreamStatus();
                if (streamStatus.equals(STREAM_READY_STATE)) {
                    log.info("Stream {} is ready...", streamName);
                    break;
                }
                else {
                    //If not ready block and wait
                    log.debug("Stream {} is still {}, waiting to retry...", streamName, streamStatus);
                    Thread.sleep(50);
                }
            } catch (ResourceNotFoundException e) {
                if (forceCreate) {
                    log.info("Stream {} does not exist yet, creating now...", streamName);
                    kinesisClient.createStream(streamName, kinesisShardCount);
                }
                else {
                    log.info("Stream {} does not exist yet, waiting to retry..", streamName);
                    Thread.sleep(50);
                }
            }
        }
    }

    /**
     * Creates a new Kinesis Worker (Consumer) that processes events from a given stream with the logic provided in the RecordProcessor
     * @param streamName
     * @param recordProcessorFactory
     * @return
     * @throws InterruptedException
     */
    public Worker getKinesisWorker(String streamName, String appName, IRecordProcessorFactory recordProcessorFactory) throws InterruptedException {
        waitForStreamActive(streamName, false);

        final KinesisClientLibConfiguration clientConfig = new KinesisClientLibConfiguration(
                appName + streamName,
                streamName,
                DefaultAWSCredentialsProviderChain.getInstance(),
                UUID.randomUUID().toString())
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON) //get the earliest available records
                .withKinesisEndpoint(String.format("https://%s:%s", kinesisEndpoint, kinesisPort))
                .withDynamoDBEndpoint(String.format("https://%s:%s", dynamoEndpoint, dynamoPort));

        return new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(clientConfig)
                .metricsFactory(metricsFactory)
                .dynamoDBClient(dynamoClient)
                .kinesisClient(getKinesisClient(kinesisEndpoint, kinesisPort , awsRegion))
                .build();
    }

    /**
     * Creates a new Kinesis Producer. This shouldn't be used directly; instead request a KinesisEventProducer from its
     * associated factory which wraps the functionality of this producer into a simple sendEvent() call
     * @return
     */
    public KinesisProducer getKinesisProducer() {
        KinesisProducerConfiguration producerConfig = new KinesisProducerConfiguration()
                .setCustomEndpoint(kinesisEndpoint)
                .setPort(kinesisPort)
                .setCredentialsProvider(DefaultAWSCredentialsProviderChain.getInstance())
                .setRegion(awsRegion)
                .setMetricsLevel(metricsLevel)
                .setVerifyCertificate(false);

        return new KinesisProducer(producerConfig);
    }
}