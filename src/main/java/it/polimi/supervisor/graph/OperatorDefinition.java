package it.polimi.supervisor.graph;

import lombok.Data;

@Data
public class OperatorDefinition {
    private Integer id;

    private Integer windowSize;

    private Integer windowSlide;

    private Aggregation aggregation;
}
