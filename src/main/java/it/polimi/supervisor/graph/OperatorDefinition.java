package it.polimi.supervisor.graph;

import lombok.Data;

@Data
public class OperatorDefinition {
    private Long id;

    private Long windowSize;

    private Long windowSlide;

    private Aggregation aggregation;
}
