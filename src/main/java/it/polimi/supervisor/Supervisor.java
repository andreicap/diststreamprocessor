package it.polimi.supervisor;

import com.google.common.collect.Streams;
import it.polimi.supervisor.graph.GraphDefinition;
import it.polimi.supervisor.graph.OperatorDefinition;
import it.polimi.supervisor.graph.exception.CycleDetectedException;
import it.polimi.supervisor.graph.exception.DuplicatedOperatorException;
import it.polimi.supervisor.worker.Address;
import it.polimi.supervisor.worker.Operator;
import it.polimi.supervisor.worker.OperatorDetails;
import it.polimi.supervisor.worker.OperatorRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Controller
public class Supervisor {

    private final Logger logger;

    private final OperatorRegistry operatorRegistry;

    private final SimpMessagingTemplate template;

    @Autowired
    public Supervisor(final Logger logger, final OperatorRegistry operatorRegistry, final SimpMessagingTemplate template) {
        this.logger = logger;
        this.operatorRegistry = operatorRegistry;
        this.template = template;
    }

    @Scheduled(fixedRate = 2000)
    public void checkWorkersState() {
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

        logger.info("Deploying graph:" + graphDefinition);
        final List<Operator> selectedOperators = selectOperators(operators, neededOperators);
        initiateOperators(selectedOperators, graphDefinition.getOperators());
        initiatePipes(selectedOperators, graphDefinition);
        selectedOperators.forEach(Operator::startStreamProcessing);
        logger.info("Successfully deployed");
    }

    private List<Operator> selectOperators(final List<Operator> operators, final int size) {
        return operators.stream()
                .limit(size)
                .collect(Collectors.toList());
    }

    private void initiateOperators(final List<Operator> selectedOperators, final List<OperatorDefinition> operatorDefinitions) {
        Streams.forEachPair(selectedOperators.stream(), operatorDefinitions.stream(), (operator, definition) -> {
            operator.setOperatorId(definition.getId());
            operator.setWindowSize(definition.getWindowSize());
            operator.setWindowSlide(definition.getWindowSlide());
            operator.setAggregation(definition.getAggregation());
        });
    }

    private void initiatePipes(final List<Operator> selectedOperators, final GraphDefinition graphDefinition) {
        selectedOperators.forEach(operator -> {
            final Integer operatorId = operator.getOperatorDetails().getOperatorId();

            final List<Integer> inputOperatorsIds = graphDefinition.getInputOperatorsIds(operatorId);
            final List<Address> inputs = mapToOperatorAddress(inputOperatorsIds, selectedOperators);
            operator.setInputs(inputs);

            final List<Integer> outputOperatorIds = graphDefinition.getOutputOperatorsIds(operatorId);
            final List<Address> outputs = mapToOperatorAddress(outputOperatorIds, selectedOperators);
            operator.setOutputs(outputs);
        });

    }

    private List<Address> mapToOperatorAddress(final List<Integer> operatorIds, final List<Operator> selectedOperators) {
        return operatorIds.stream()
                .map(operatorId -> getOperatorAddress(operatorId, selectedOperators))
                .collect(Collectors.toList());
    }

    private Address getOperatorAddress(final Integer operatorId, final List<Operator> operators) {
        return operators.stream()
                .filter(operator -> operator.getOperatorDetails().getOperatorId().equals(operatorId))
                .findFirst()
                .map(Operator::getAddress)
                .get();
    }
}
