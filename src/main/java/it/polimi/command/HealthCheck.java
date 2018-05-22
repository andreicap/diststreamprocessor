package it.polimi.command;

import it.polimi.OperatorNode;

public class HealthCheck implements Command {
    @Override
    public Object execute() {
        return OperatorNode.state;
    }
}
