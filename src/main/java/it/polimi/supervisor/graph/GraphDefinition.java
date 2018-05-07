package it.polimi.supervisor.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.io.IOException;
import java.util.List;

@Data
public class GraphDefinition {

    private List<OperatorDefinition> operators;

    private List<PipeDefinition> pipes;

    public static GraphDefinition fromString(final String json) throws IOException {
        return new ObjectMapper().readValue(json, GraphDefinition.class);
    }
}