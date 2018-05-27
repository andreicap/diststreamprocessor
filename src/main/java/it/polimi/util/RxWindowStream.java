package it.polimi.util;

import com.google.common.collect.Lists;

import java.net.Socket;
import java.util.List;
import java.util.function.Consumer;

public class RxWindowStream<T> {

    private final Socket socket;

    private final List<T> buffer;

    private int unSlided;

    private int windowSize;

    private int windowSlide;

    private Consumer<List<T>> consumer;

    private Runnable runnable;

    public RxWindowStream(final Socket socket) {
        this.socket = socket;
        this.buffer = Lists.newLinkedList();
        this.unSlided = 0;
        this.windowSize = 1;
        this.windowSlide = 1;
    }

    public RxWindowStream<T> withWindowSize(final int windowSize) {
        if (windowSize < 1) {
            throw new IllegalArgumentException("Window size cannot be smaller than 1");
        }
        this.windowSize = windowSize;
        return this;
    }

    public RxWindowStream<T> withWindowSlide(final int windowSlide) {
        if (windowSlide < 1) {
            throw new IllegalArgumentException("Window slide cannot be smaller than 1");
        }
        this.windowSlide = windowSlide;
        return this;
    }

    public RxWindowStream<T> subscribe(final Consumer<List<T>> consumer) {
        this.consumer = consumer;
        return this;
    }

    public RxWindowStream<T> onEndOfStream(final Runnable runnable) {
        this.runnable = runnable;
        return this;
    }

    public void start() {
        new RxObjectInputStream(socket)
                .onException((e) -> runnable.run())
                .subscribe((value) -> consume((T) value))
                .start();
    }

    private void consume(final T value) {
        buffer.add(value);
        if (unSlided > 0) {
            if (unSlided <= buffer.size()) {
                buffer.subList(0, unSlided).clear();
            } else {
                return;
            }
        }

        if (buffer.size() < windowSize) {
            return;
        }

        final List<T> window = Lists.newArrayList(buffer.subList(0, windowSize));

        if (windowSlide <= windowSize || buffer.size() >= windowSize + windowSlide) {
            buffer.subList(0, windowSlide).clear();
        } else {
            // buffer.size() < windowSize + windowSlide
            buffer.clear();
            unSlided = windowSlide - buffer.size();
        }

        consumer.accept(window);
    }
}
