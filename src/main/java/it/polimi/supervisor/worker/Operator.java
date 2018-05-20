package it.polimi.supervisor.worker;

import it.polimi.command.Command;
import it.polimi.command.HealthCheck;
import it.polimi.command.ProcessStream;
import it.polimi.supervisor.Logger;
import it.polimi.supervisor.graph.Aggregation;
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

    private final Long registrationId;

    private final Socket socket;

    private final ObjectOutputStream output;

    private final Logger logger;

    private final ScheduledFuture<?> future;

    private State state = State.FREE;

    @Setter
    private Integer operatorId;

    @Setter
    private Long windowSize;

    @Setter
    private Long windowSlide;

    @Setter
    private Aggregation aggregation;

    @Setter
    private List<Address> inputs;

    @Setter
    private List<Address> outputs;

    Operator(final Long registrationId, final Socket socket, final Logger logger) throws IOException {
        this.registrationId = registrationId;
        this.socket = socket;
        this.logger = logger;
        this.output = new ObjectOutputStream(socket.getOutputStream());

        final ScheduledExecutorService service = Executors.newScheduledThreadPool(2);
        service.execute(this::handleResponse);
        this.future = service.scheduleAtFixedRate(() -> executeRemoteCommand(new HealthCheck()), 0, 2, TimeUnit.SECONDS);
    }

    private void handleResponse() {
        try {
            final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            while (true) {
                log.info("reading");
                final Object response = input.readObject();
                log.info(response.toString());
            }
        } catch (IOException | ClassNotFoundException e) {
            state = State.CRUSHED;
            e.printStackTrace();
        }
    }

    public void startStreamProcessing() {
        executeRemoteCommand(new ProcessStream(operatorId, windowSize, windowSlide, aggregation, inputs, outputs));
    }

    private void executeRemoteCommand(final Command command) {
        try {
            log.info("executing command: " + command.toString());
            output.writeObject(command);
            log.info("executed");
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
                .withHost(socket.getInetAddress().getHostName())
                .withPort(socket.getPort())
                .withState(state)
                .withOperatorId(operatorId)
                .withWindowSize(windowSize)
                .withWindowSlide(windowSlide)
                .withAggregation(aggregation);
    }

    public Address getAddress() {
        return new Address(socket.getInetAddress().getHostName(), socket.getPort());
    }
}
