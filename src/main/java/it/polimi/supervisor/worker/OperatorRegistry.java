package it.polimi.supervisor.worker;

import com.google.common.collect.Lists;
import it.polimi.supervisor.Logger;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

@Component
public class OperatorRegistry {

    private final Logger logger;

    @Getter
    private final List<Operator> operators;

    private ServerSocket serverSocket;

    @Value("${worker.registry.port}")
    private Integer port;

    @Autowired
    OperatorRegistry(final Logger logger) {
        this.logger = logger;
        this.operators = Lists.newLinkedList();
        this.serverSocket = null;
    }

    @PostConstruct
    void init() {
        new Thread(this::listenForOperators).start();
    }

    private void listenForOperators() {
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Listening for operators on: " + serverSocket.getInetAddress());
            while (true) {
                final Socket clientSocket = serverSocket.accept();
                final long registrationId = operators.size() + 1;
                registerOperator(registrationId, clientSocket);
            }
        } catch (IOException e) {
            logger.severe("OperatorRegistry failed.");
            e.printStackTrace();
        }
    }

    private void registerOperator(final long registrationId, final Socket clientSocket) {
        try {
            logger.info("Registering " + clientSocket.toString());
            operators.add(new Operator(registrationId, clientSocket, logger));
            logger.info("New operator registered " + clientSocket.toString());
        } catch (IOException e) {
            logger.warn("Failed to register new operator " + clientSocket.toString());
            e.printStackTrace();
        }
    }
}
