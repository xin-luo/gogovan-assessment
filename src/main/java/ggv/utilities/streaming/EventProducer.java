package ggv.utilities.streaming;

public interface EventProducer<T> {
    public void sendEvent(T event);
}
