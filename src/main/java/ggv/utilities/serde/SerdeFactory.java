package ggv.utilities.serde;

public abstract class SerdeFactory {
    public <E> ByteSerializer<E> getSerializer(Class<E> clazz) {
        throw new RuntimeException("Serializer getter needs to be implemented by child class.");
    }

    public <E> ByteDeserializer<E> getDeserializer(Class<E> clazz) {
        throw new RuntimeException("Deserializer getter needs to be implemented by child class.");
    }
}
