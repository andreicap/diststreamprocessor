package it.polimi.supervisor.worker;

import it.polimi.command.Command;
import it.polimi.command.Deploy;
import it.polimi.command.HealthCheck;
import it.polimi.command.StartProcessing;
import it.polimi.supervisor.Logger;
import it.polimi.supervisor.graph.Aggregation;
import it.polimi.util.RxObjectInputStream;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.java.Log;

import java.io.IOException;
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

    private ScheduledFuture<?> future;

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

        handleResponses();
        scheduledHealthCheck();
    }

    private void handleResponses() {
        new RxObjectInputStream(socket)
                .subscribe((port) -> address = new Address(socket.getInetAddress().getHostName(), port), Integer.class)
                .subscribe((input) -> state = input, State.class)
                .onException((e) -> {
                    state = State.CRUSHED;
                    e.printStackTrace();
                })
                .start();
    }

    private void scheduledHealthCheck() {
        final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        this.future = service.scheduleAtFixedRate(() -> executeRemoteCommand(new HealthCheck()), 0, 1, TimeUnit.SECONDS);
    }

    public void deploy() {
        executeRemoteCommand(new Deploy(outputsAdresses));
    }

    public void startProcessing() {
        executeRemoteCommand(new StartProcessing(operatorId, windowSize, windowSlide, aggregation, inputsNumber));
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
