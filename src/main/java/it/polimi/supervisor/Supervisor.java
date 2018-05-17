package it.polimi.supervisor;

import it.polimi.supervisor.graph.GraphDefinition;
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
                .sorted(Comparator.comparingLong(OperatorDetails::getOperatorId))
                .collect(Collectors.toList());
    }

    @MessageMapping("/deploy")
    public void deploy(final GraphDefinition graphDefinition) {
        final List<Operator> operators = operatorRegistry.getOperators();
        graphDefinition.validateGraph();
        if (operators.size() < graphDefinition.getOperators().size()) {
            logger.warn("Not enough operators");
            return;
        }

        logger.info("Deploying graph:" + graphDefinition);
        // TODO initiate operators
    }
}
