package it.polimi.panel;

import it.polimi.panel.dto.GraphDefinitionDTO;
import it.polimi.panel.dto.MachineDetailsDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.util.Collections;
import java.util.List;

@Slf4j
@Controller
public class PanelController {

    private final SimpMessagingTemplate template;

    @Autowired
    public PanelController(final SimpMessagingTemplate template) {
        this.template = template;
    }

    @Scheduled(fixedRate = 5000)
    public void getAvailableMachinesDetails() {
        log.info("Publishing available machines details.");

        final List<MachineDetailsDTO> machineDetails = Collections.singletonList(new MachineDetailsDTO()
                .withMachineId(1L)
                .withHost("192.168.1.1")
                .withPort("1234"));

        template.convertAndSend("/topic/machines", machineDetails);
    }

    @MessageMapping("/deploy")
    public void deploy(final GraphDefinitionDTO definitionDTO) {
        log.info("Deploying " + definitionDTO);

        throw new UnsupportedOperationException();
    }
}