package ggv.utilities.serde;

public interface ByteSerializer<T> {
    byte[] serialize(T input);
}
