package ggv.utilities.serde;

import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class AvroSerdeFactory extends SerdeFactory {
    private static final Map<Class<?>, AvroSerde<?>> avroSerdes = new HashMap<>();

    public static <E> AvroSerde<E> get(Class<E> clazz) {
        return (AvroSerde<E>) avroSerdes.computeIfAbsent(clazz, key -> new AvroSerde<>(clazz));
    }

    public <F> AvroSerde<F> getSerializer(Class<F> clazz) {
        return get(clazz);
    }

    public <F> AvroSerde<F> getDeserializer(Class<F> clazz) {
        return get(clazz);
    }
}
