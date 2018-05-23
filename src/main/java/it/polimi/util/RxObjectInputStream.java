package it.polimi.util;

import com.google.common.collect.Maps;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.Map;
import java.util.function.Consumer;

public class RxObjectInputStream {

    private final Socket socket;

    private Map<Class<?>, Consumer<?>> consumers = Maps.newLinkedHashMap();

    private Consumer<? super IOException> ioExceptionConsumer = Throwable::printStackTrace;

    private Consumer<? super EOFException> eofExceptionConsumer = (e) -> {};

    private Consumer<? super ClassNotFoundException> classNotFoundExceptionConsumer = Throwable::printStackTrace;

    public RxObjectInputStream(final Socket socket) {
        this.socket = socket;
    }

    public RxObjectInputStream subscribe(final Consumer<Object> consumer) {
        return subscribe(consumer, Object.class);
    }

    public <T> RxObjectInputStream subscribe(final Consumer<T> consumer, final Class<T> objectClass) {
        consumers.put(objectClass, consumer);
        return this;
    }

    public RxObjectInputStream onIOException(final Consumer<IOException> ioExceptionConsumer) {
        this.ioExceptionConsumer = ioExceptionConsumer;
        return this;
    }

    public RxObjectInputStream onEOFException(final Consumer<EOFException> eofExceptionConsumer) {
        this.eofExceptionConsumer = eofExceptionConsumer;
        return this;
    }

    public RxObjectInputStream onClassNotFoundException(final Consumer<ClassNotFoundException> classNotFoundExceptionConsumer) {
        this.classNotFoundExceptionConsumer = classNotFoundExceptionConsumer;
        return this;
    }

    public RxObjectInputStream onException(final Consumer<Exception> exceptionConsumer) {
        this.ioExceptionConsumer = exceptionConsumer;
        this.eofExceptionConsumer = exceptionConsumer;
        this.classNotFoundExceptionConsumer = exceptionConsumer;
        return this;
    }

    public void start() {
        new Thread(this::readAll).start();
    }

    private void readAll() {
        try {
            final ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
            while (true) {
                Object object = objectInputStream.readObject();
                findConsumer(object).accept(object);
            }
        } catch (EOFException e) {
            eofExceptionConsumer.accept(e);
        } catch (IOException e) {
            ioExceptionConsumer.accept(e);
        } catch (ClassNotFoundException e) {
            classNotFoundExceptionConsumer.accept(e);
        }
    }

    private <T> Consumer<T> findConsumer(final T value) {
        if (consumers.containsKey(value.getClass())) {
            return (Consumer<T>) consumers.get(value.getClass());
        }

        return (Consumer<T>) consumers.entrySet()
                .stream()
                .filter((entry) -> entry.getKey().isInstance(value))
                .map((entry) -> entry.getValue())
                .findFirst()
                .orElse((t) -> {
                });
    }
}
