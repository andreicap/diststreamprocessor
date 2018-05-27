package it.polimi.command;

import it.polimi.OperatorNode;
import it.polimi.supervisor.worker.Address;
import it.polimi.supervisor.worker.State;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class Deploy implements Command {

    private final List<Address> outputsAdresses;

    @Override
    public Object execute() {
        initiateOutputStreams(outputsAdresses);
        OperatorNode.state = State.READY;
        return OperatorNode.state;
    }

    private static void initiateOutputStreams(final List<Address> outputsAdresses) {
        OperatorNode.outputStreams = outputsAdresses.stream()
                .map(Address::getOptionalObjectOutputStream)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }
}
