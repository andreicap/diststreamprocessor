package it.polimi.supervisor;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
class WorkerRegistry extends Thread {

    private final Logger logger;

    private final AtomicBoolean running;

    @Getter
    private final List<Worker> workers;

    private ServerSocket serverSocket;

    @Value("${worker.registry.port}")
    private Integer port;

    @Autowired
    WorkerRegistry(final Logger logger) {
        this.logger = logger;
        this.running = new AtomicBoolean(true);
        this.workers = new LinkedList<>();
        this.serverSocket = null;
    }

    @PostConstruct
    void init() {
        start();
    }

    @Override
    public void run() {
        try {
            this.serverSocket = new ServerSocket(port);
            logger.info("Listening for workers on: " + serverSocket.getInetAddress());
            while (running.get()) {
                final Socket clientSocket = serverSocket.accept();
                final long workerId = workers.size() + 1;
                workers.add(new Worker(workerId, clientSocket));
                logger.info("New worker registered " + clientSocket.toString());
            }
        } catch (IOException e) {
            logger.severe("WorkerRegistry failed.");
            e.printStackTrace();
        }
    }

    void kill() {
        running.set(false);
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.severe("WorkerRegistry failed to close.");
            e.printStackTrace();
        }
    }
}
