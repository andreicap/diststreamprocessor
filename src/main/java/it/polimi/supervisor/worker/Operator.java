package it.polimi.supervisor.worker;

import it.polimi.command.HealthCheck;
import it.polimi.supervisor.Logger;
import lombok.extern.java.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

@Log
public class Operator {

    private static final long SLEEP_TIME = 2000;

    private final Long operatorId;

    private final Socket socket;

    private final Logger logger;

    private State state;

    Operator(final Long operatorId, final Socket socket, final Logger logger) {
        this.operatorId = operatorId;
        this.socket = socket;
        this.logger = logger;
        this.state = State.FREE;

        new Thread(this::periodicallySendHealthCheck).start();
        new Thread(this::handleResponse).start();
    }

    public OperatorDetails getOperatorDetails() {
        return new OperatorDetails()
                .withOperatorId(operatorId)
                .withHost(socket.getInetAddress().getHostName())
                .withPort(socket.getPort())
                .withState(state);
    }

    private void periodicallySendHealthCheck() {
        try {
            final ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
            while (true) {
                output.writeObject(new HealthCheck());
                Thread.sleep(SLEEP_TIME);
            }
        } catch (IOException e) {
            state = State.CRUSHED;
            logger.warn("Operator " + operatorId + " crushed");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void handleResponse() {
        try {
            final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            while (true) {
                final Object response = input.readObject();
//                log.info(response.toString());
            }
        } catch (IOException | ClassNotFoundException e) {
            state = State.CRUSHED;
        }
    }
}
