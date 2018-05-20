package it.polimi.supervisor.worker;

import lombok.Value;

import java.io.Serializable;

@Value
public class Address implements Serializable {
    private final String host;

    private final Integer port;
}
