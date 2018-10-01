package ggv.utilities;

import com.amazonaws.services.kinesis.model.AmazonKinesisException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * This is a wrapper for the Kinesis Producer Library which makes it straightforward to send a Json-encoded message
 * to a particular Kinesis stream
 */
@Slf4j
public class KinesisEventProducer<T> {
    private final String streamName;
    private final KinesisProducer producer;
    private final Schema schema;

    protected KinesisEventProducer(String streamName, KinesisUtilities utilities, Class<T> clazz) {
        this.streamName = streamName;
        producer = utilities.getKinesisProducer();

        schema = ReflectData.get().getSchema(clazz);

        try {
            utilities.waitForStreamActive(streamName, false);
        } catch (LimitExceededException e) {
            log.error("Kinesis shard limits exceeded: ", e);
        } catch (AmazonKinesisException e) {
            log.error("Kinesis Client Error: {}", e);
        } catch (InterruptedException e) {
            log.error("Interrupted while creating stream: {}", e);
        }
    }

    /**
     * Provided methods for sending an event to Kinesis with or without the partition key explicitly defined
     * (If not defined, use a random UUID as the partition key)
     * Returns true if event has been accepted into the events queue. Note that a true return value doesn't necessarily mean
     * the event is guaranteed to be sent to Kinesis if the application crashes between the sendEvent call and the SendMessagesRunnable getting called
     */
    public void sendEvent(T object) {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        try {
            DatumWriter<T> writer = new ReflectDatumWriter<>(schema);
            writer.write(object, binaryEncoder);
            binaryEncoder.flush();
            byteArrayOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            log.error("IO Exception while attempting to serialize object {}: {}", object, e);
        }

        sendEvent(byteArrayOutputStream.toByteArray(), UUID.randomUUID().toString());
    }

    private void sendEvent(byte[] message, String key) {
        ListenableFuture<UserRecordResult> futureResultStatus = producer.addUserRecord(streamName, key, ByteBuffer.wrap(message));

        Futures.addCallback(futureResultStatus, new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                log.error("Producer failed to send event: ", t);

                if (t instanceof UserRecordFailedException) {
                    Attempt attempt = ((UserRecordFailedException) t).getResult().getAttempts().get(0);

                    String error = String.format("Delay after prev attempt: %d ms, Duration: %d ms, Code: %s, Message: %s",
                            attempt.getDelay(), attempt.getDuration(), attempt.getErrorCode(), attempt.getErrorMessage());

                    log.error(String.format("Record failed to put, partitionKey=%s, payload=%s, attempt=%s", key, message, error));
                }
            }

            @Override
            public void onSuccess(UserRecordResult result) {
                log.trace("Producer successfully sent event: {}", result);
            }
        });
    }
}