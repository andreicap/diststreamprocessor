package it.polimi.command;

import java.io.Serializable;

public interface Command extends Serializable {
    Object execute();
}
