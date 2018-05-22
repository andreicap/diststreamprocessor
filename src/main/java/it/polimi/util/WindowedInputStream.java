package it.polimi.util;

import com.google.common.collect.Lists;
import lombok.extern.java.Log;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.List;
import java.util.stream.DoubleStream;

@Log
public class WindowedInputStream {

    private final Socket socket;

    private final List<Double> buffer;

    private int unSlided;

    public WindowedInputStream(final Socket socket) {
        this.socket = socket;
        this.buffer = Lists.newLinkedList();
        this.unSlided = 0;

        new Thread(this::readAll).start();
    }

    private void readAll() {
        try {
            final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());

            while (true) {
                try {
                    synchronized (this) {
                        buffer.add(input.readDouble());
                    }
                } catch (EOFException e) {
                    log.info("Stream from " + socket + " has ended.");
                    input.close();
                    socket.close();
                    return;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    synchronized public DoubleStream getWindow(final int size, final int slide) {
        if (size < 1) {
            throw new IllegalArgumentException("Window size cannot be smaller than 1");
        }
        if (slide < 1) {
            throw new IllegalArgumentException("Window slide cannot be smaller than 1");
        }
        if (unSlided > 0) {
            if (unSlided <= buffer.size()) {
                buffer.subList(0, unSlided).clear();
            } else {
                return DoubleStream.empty();
            }
        }

        if (buffer.size() < size) {
            return DoubleStream.empty();
        }

        final DoubleStream window = Lists.newArrayList(buffer.subList(0, size)).stream().mapToDouble(Double::doubleValue);

        if (slide <= size || buffer.size() >= size + slide) {
            buffer.subList(0, slide).clear();
        } else {
            // buffer.size() < size + slide
            buffer.clear();
            unSlided = slide - buffer.size();
        }

        return window;
    }

    synchronized public boolean isEmpty(final int size) {
        return socket.isClosed() && buffer.size() - unSlided < size;
    }
}
