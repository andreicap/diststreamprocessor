package it.polimi.command;

import it.polimi.supervisor.graph.Aggregation;
import it.polimi.supervisor.worker.Address;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public class ProcessStream implements Command {

    private final Integer operatorId;

    private final Long windowSize;

    private final Long windowSlide;

    private final Aggregation aggregation;

    private final List<Address> inputs;

    private final List<Address> outputs;

    @Override
    public Object execute() {
        return null;
    }
}
