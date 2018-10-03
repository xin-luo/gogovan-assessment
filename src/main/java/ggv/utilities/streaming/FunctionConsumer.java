package ggv.utilities.streaming;

import java.util.function.Function;

public interface FunctionConsumer {
    <E> void addFunction(Class<E> type, Function<E, ?> function);
}
