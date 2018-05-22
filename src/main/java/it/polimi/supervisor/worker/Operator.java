package it.polimi.supervisor.worker;

import it.polimi.command.Command;
import it.polimi.command.Deploy;
import it.polimi.command.HealthCheck;
import it.polimi.command.StartProcessing;
import it.polimi.supervisor.Logger;
import it.polimi.supervisor.graph.Aggregation;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.java.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Log
public class Operator {

    @Getter
    private final Long registrationId;

    private final Socket socket;

    private final Logger logger;

    private final ObjectOutputStream output;

    private final ScheduledFuture<?> future;

    @Getter
    private State state = State.FREE;

    @Setter
    @Getter
    private Integer operatorId;

    @Setter
    @Getter
    private Integer windowSize;

    @Setter
    @Getter
    private Integer windowSlide;

    @Setter
    @Getter
    private Aggregation aggregation;

    @Setter
    @Getter
    private Integer inputsNumber;

    @Setter
    @Getter
    private List<Address> outputsAdresses;

    @Getter
    private Address address;

    Operator(final Long registrationId, final Socket socket, final Logger logger) throws IOException {
        this.registrationId = registrationId;
        this.socket = socket;
        this.logger = logger;
        this.output = new ObjectOutputStream(socket.getOutputStream());

        new Thread(this::handleResponses).start();

        final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        this.future = service.scheduleAtFixedRate(() -> executeRemoteCommand(new HealthCheck()), 0, 60, TimeUnit.SECONDS);
    }

    public void deploy() {
        executeRemoteCommand(new Deploy(outputsAdresses));
    }

    public void startProcessing() {
        executeRemoteCommand(new StartProcessing(operatorId, windowSize, windowSlide, aggregation, inputsNumber));
    }

    private void handleResponses() {
        try {
            final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            while (true) {
                final Object response = input.readObject();
                log.info("Message arrived: " + response.toString());
                if (response instanceof Address) {
                    address = (Address) response;
                } else if (response instanceof State) {
                    state = (State) response;
                } else {
                    log.warning("Unknown message " + response.toString());
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            state = State.CRUSHED;
            e.printStackTrace();
        }
    }

    private void executeRemoteCommand(final Command command) {
        try {
            log.info("Executing command: " + command.toString());
            output.writeObject(command);
            log.info("Executed command: " + command.toString());
        } catch (IOException e) {
            state = State.CRUSHED;
            future.cancel(true);
            logger.warn("Operator " + registrationId + " crushed");
            e.printStackTrace();
        }
    }

    public OperatorDetails getOperatorDetails() {
        return new OperatorDetails()
                .withRegistrationId(registrationId)
                .withHost(address != null ? address.getHost() : null)
                .withPort(address != null ? address.getPort() : null)
                .withState(state)
                .withOperatorId(operatorId)
                .withWindowSize(windowSize)
                .withWindowSlide(windowSlide)
                .withAggregation(aggregation);
    }
}
