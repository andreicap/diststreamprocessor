package it.polimi.supervisor;

import it.polimi.supervisor.graph.GraphDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Controller
public class Supervisor {

    private final Logger logger;

    private final WorkerRegistry workerRegistry;

    private final SimpMessagingTemplate template;

    @Autowired
    public Supervisor(final Logger logger, final WorkerRegistry workerRegistry, final SimpMessagingTemplate template) {
        this.logger = logger;
        this.workerRegistry = workerRegistry;
        this.template = template;
    }

    @Scheduled(fixedRate = 2000)
    public void checkWorkersState() {
        final List<WorkerDetails> workerDetails = getWorkersDetails();
        template.convertAndSend("/topic/workers", workerDetails);
    }

    private List<WorkerDetails> getWorkersDetails() {
        final List<Worker> workers = workerRegistry.getWorkers();
        return workers.parallelStream()
                .map(Worker::getWorkerDetails)
                .sorted(Comparator.comparingLong(WorkerDetails::getWorkerId))
                .collect(Collectors.toList());
    }

    @MessageMapping("/deploy")
    public void deploy(final GraphDefinition graphDefinition) {
        logger.info("Deploying graph:" + graphDefinition);


    }
}
