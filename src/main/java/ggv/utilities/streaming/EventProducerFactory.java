package ggv.utilities.streaming;

public interface EventProducerFactory {
    public <E> EventProducer<E> get(Class<E> clazz);
}
