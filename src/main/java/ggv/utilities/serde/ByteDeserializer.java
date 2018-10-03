package ggv.utilities.serde;

public interface ByteDeserializer<T> {
    static <E> ByteSerializer get(Class<E> clazz) {
        return null;
    }
    T deserialize(byte[] input);
}
