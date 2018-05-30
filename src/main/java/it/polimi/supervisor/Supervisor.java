package it.polimi.supervisor;

import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import it.polimi.supervisor.graph.GraphDefinition;
import it.polimi.supervisor.graph.OperatorDefinition;
import it.polimi.supervisor.graph.exception.CycleDetectedException;
import it.polimi.supervisor.graph.exception.DuplicatedOperatorException;
import it.polimi.supervisor.worker.*;
import it.polimi.util.RxObjectInputStream;
import org.awaitility.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

@Controller
public class Supervisor {

    private static final int ANY_PORT = 0;

    private final Logger logger;

    private final OperatorRegistry operatorRegistry;

    private final SimpMessagingTemplate template;

    @Autowired
    public Supervisor(final Logger logger, final OperatorRegistry operatorRegistry, final SimpMessagingTemplate template) {
        this.logger = logger;
        this.operatorRegistry = operatorRegistry;
        this.template = template;
    }

    @Scheduled(fixedRate = 500)
    public void checkOperatorsState() {
        final List<OperatorDetails> operatorDetails = getOperatorsDetails();
        template.convertAndSend("/topic/operators", operatorDetails);
    }

    private List<OperatorDetails> getOperatorsDetails() {
        final List<Operator> operators = operatorRegistry.getOperators();
        return operators.stream()
                .map(Operator::getOperatorDetails)
                .sorted(Comparator.comparingLong(OperatorDetails::getRegistrationId))
                .collect(Collectors.toList());
    }

    @MessageMapping("/deploy")
    public void deploy(final GraphDefinition graphDefinition) {
        try {
            graphDefinition.validate();
        } catch (DuplicatedOperatorException exc) {
            logger.warn("Uploaded graph is not valid - duplicated operators ids");
            return;
        } catch (CycleDetectedException exc) {
            logger.warn("Uploaded graph is not valid - cycle detected");
            return;
        }

        final List<Operator> operators = operatorRegistry.getOperators();
        final int availableOperators = operators.size();
        final int neededOperators = graphDefinition.getOperators().size();
        if (availableOperators < neededOperators) {
            logger.warn("Not enough operators");
            return;
        }

        final List<Operator> selectedOperators = selectOperators(operators, neededOperators);
        deploy(selectedOperators, graphDefinition);
    }

    private List<Operator> selectOperators(final List<Operator> operators, final int size) {
        return operators.stream()
                .filter(operator -> operator.getState() == State.FREE)
                .limit(size)
                .collect(Collectors.toList());
    }

    private void deploy(final List<Operator> selectedOperators, final GraphDefinition graphDefinition) {
        try {
            logger.info("Deploying graph:" + graphDefinition);
            initiateOperators(selectedOperators, graphDefinition.getOperators());
            initiatePipelines(selectedOperators, graphDefinition);
            initiateTarget(selectedOperators, graphDefinition.getLeavesIds());
            selectedOperators.forEach(Operator::deploy);
            waitUntilAllOperatorsAreReady(selectedOperators);
            selectedOperators.forEach(Operator::startProcessing);
            initiateSource(selectedOperators, graphDefinition);
            logger.info("Successfully deployed");
        } catch (Exception e) {
            logger.warn("Deployment failed");
            e.printStackTrace();
        }
    }

    private void initiateOperators(final List<Operator> selectedOperators, final List<OperatorDefinition> operatorDefinitions) {
        Streams.forEachPair(selectedOperators.stream(), operatorDefinitions.stream(), (operator, definition) -> {
            operator.setOperatorId(definition.getId());
            operator.setWindowSize(definition.getWindowSize());
            operator.setWindowSlide(definition.getWindowSlide());
            operator.setAggregation(definition.getAggregation());
        });
    }

    private void initiatePipelines(final List<Operator> selectedOperators, final GraphDefinition graphDefinition) {
        selectedOperators.forEach(operator -> {
            final List<Address> outputs = graphDefinition.getOutputOperatorsIds(operator.getOperatorId())
                    .stream()
                    .map(operatorId -> getOperatorAddress(operatorId, selectedOperators))
                    .collect(Collectors.toList());
            operator.setOutputsAdresses(outputs);
            final int inputsNumber = graphDefinition.getInputOperatorsIds(operator.getOperatorId()).size();
            operator.setInputsNumber(inputsNumber != 0 ? inputsNumber : 1);
        });
    }

    private Address getOperatorAddress(final Integer operatorId, final List<Operator> operators) {
        return operators.stream()
                .filter(operator -> operator.getOperatorId().equals(operatorId))
                .findFirst()
                .map(Operator::getAddress)
                .get();
    }

    private void initiateTarget(final List<Operator> selectedOperators, final List<Integer> leafOperatorIds) throws IOException {
        final ServerSocket targetServerSocket = new ServerSocket(ANY_PORT);
        final String host = InetAddress.getLocalHost().getHostAddress();
        final Address targetAddress = new Address(host, targetServerSocket.getLocalPort());
        selectedOperators.stream()
                .filter(operator -> leafOperatorIds.contains(operator.getOperatorId()))
                .forEach(operator -> operator.getOutputsAdresses().add(targetAddress));
        new Thread(() -> collectOutputAndLog(targetServerSocket, leafOperatorIds.size())).start();
    }

    private void collectOutputAndLog(final ServerSocket targetServerSocket, final int leafOperators) {
        final List<Double> finalOutput = Collections.synchronizedList(Lists.newArrayList());
        final AtomicInteger counter = new AtomicInteger(0);
        try {
            for (int i = 0; i < leafOperators; i++) {
                final Socket leafOperatorSocket = targetServerSocket.accept();
                new RxObjectInputStream(leafOperatorSocket)
                        .onException((e) -> counter.incrementAndGet())
                        .subscribe(finalOutput::add, Double.class)
                        .start();
            }

            await().until(() -> counter.get() == leafOperators);
            logger.info("Output: " + finalOutput);
            targetServerSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void waitUntilAllOperatorsAreReady(final List<Operator> selectedOperators) {
        await()
                .atMost(Duration.ONE_SECOND)
                .until(() -> selectedOperators.stream()
                        .map(Operator::getState)
                        .allMatch(state -> state == State.READY));
    }

    private void initiateSource(final List<Operator> selectedOperators, final GraphDefinition graphDefinition) {
        final List<Integer> rootOperatorIds = graphDefinition.getRootIds();
        final List<Address> addresses = rootOperatorIds.stream()
                .map(rootOperatorId -> getOperatorAddress(rootOperatorId, selectedOperators))
                .collect(Collectors.toList());

        streamInput(addresses, graphDefinition.getInputValues(), graphDefinition.getDelay());
    }

    private void streamInput(final List<Address> addresses, final List<Double> inputValues, final Integer delay) {
        addresses.forEach(address -> new Thread(() -> streamInput(address, inputValues, delay)).start());
    }

    private void streamInput(final Address address, final List<Double> inputValues, final Integer delay) {
        try {
            final Socket socket = new Socket(address.getHostOrLocalhostIfOnSameMachine(), address.getPort());
            final ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());

            inputValues.forEach(i -> {
                try {
                    Thread.sleep(delay);
                    output.writeObject(i);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
            output.flush();
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
