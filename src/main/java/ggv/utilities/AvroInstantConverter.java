package ggv.utilities;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.Instant;

public class AvroInstantConverter extends Conversion<Instant> {
    @Override
    public Class<Instant> getConvertedType() {
        return Instant.class;
    }

    @Override
    public String getLogicalTypeName() {
        return "timestamp-millis";
    }

    @Override
    // convert Instant to Long
    public Long toLong(Instant value, Schema schema, LogicalType type) {
        return value.toEpochMilli();
    }

    @Override
    // parse Instant from Long
    public Instant fromLong(Long value, Schema schema, LogicalType type) {
        return Instant.ofEpochMilli(value);
    }

    @Override
    public Schema getRecommendedSchema() {
        return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    }
}
