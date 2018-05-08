package it.polimi.supervisor.worker;

import it.polimi.supervisor.Logger;
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
public class OperatorRegistry {

    private final Logger logger;

    private final AtomicBoolean running;

    @Getter
    private final List<Operator> operators;

    private ServerSocket serverSocket;

    @Value("${worker.registry.port}")
    private Integer port;

    @Autowired
    OperatorRegistry(final Logger logger) {
        this.logger = logger;
        this.running = new AtomicBoolean(true);
        this.operators = new LinkedList<>();
        this.serverSocket = null;
    }

    @PostConstruct
    void init() {
        new Thread(() -> {
            try {
                serverSocket = new ServerSocket(port);
                logger.info("Listening for operators on: " + serverSocket.getInetAddress());
                while (running.get()) {
                    final Socket clientSocket = serverSocket.accept();
                    final long workerId = operators.size() + 1;
                    operators.add(new Operator(workerId, clientSocket, logger));
                    logger.info("New operator registered " + clientSocket.toString());
                }
            } catch (IOException e) {
                logger.severe("OperatorRegistry failed.");
                e.printStackTrace();
            }
        }).start();
    }

    void kill() {
        running.set(false);
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.severe("OperatorRegistry failed to close.");
            e.printStackTrace();
        }
    }
}
