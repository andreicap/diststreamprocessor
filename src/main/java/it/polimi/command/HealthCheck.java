package it.polimi.command;

import it.polimi.supervisor.worker.State;

public class HealthCheck implements Command {
    @Override
    public Object execute() {
        return State.FREE;
    }
}
