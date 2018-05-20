package it.polimi.supervisor.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.polimi.supervisor.graph.exception.CycleDetectedException;
import it.polimi.supervisor.graph.exception.DuplicatedOperatorException;
import lombok.Data;
import lombok.extern.java.Log;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Log
@Data
public class GraphDefinition {

    private List<OperatorDefinition> operators;

    private List<PipeDefinition> pipes;

    public static GraphDefinition fromString(final String json) throws IOException {
        return new ObjectMapper().readValue(json, GraphDefinition.class);
    }

    public void validate() {
        log.info("Graph validation started");
        if (isDuplicatedId()) {
            throw new DuplicatedOperatorException();
        }
        if (isCyclic()) {
            throw new CycleDetectedException();
        }
    }

    public List<Integer> getInputOperatorsIds(final Integer operatorId) {
        return pipes.stream()
                .filter(pipe -> pipe.getOutput().equals(operatorId))
                .map(PipeDefinition::getInput)
                .collect(Collectors.toList());
    }

    public List<Integer> getOutputOperatorsIds(final Integer operatorId) {
        return pipes.stream()
                .filter(pipe -> pipe.getInput().equals(operatorId))
                .map(PipeDefinition::getOutput)
                .collect(Collectors.toList());
    }

    private boolean isDuplicatedId() {
        final Set<Integer> uniqueIds = operators.stream()
                .map(OperatorDefinition::getId)
                .collect(Collectors.toSet());
        return operators.size() != uniqueIds.size();
    }

    private boolean isCyclic() {
        // Mark all the vertices as not visited and
        // not part of recursion stack
        int V = operators.size();
        boolean[] visited = new boolean[V];
        boolean[] stack = new boolean[V];

        // Call the recursive helper function to
        // detect cycle in different DFS trees
        for (int i = 0; i < V; i++) {
            if (isCyclicUtil(i, visited, stack)) {
                return true;
            }
        }

        return false;
    }

    private boolean isCyclicUtil(final int i, boolean[] visited, boolean[] stack) {
        if (stack[i]) {
            return true;
        }

        if (visited[i]) {
            return false;
        }
        visited[i] = true;

        stack[i] = true;
        final List<Integer> children = getChildren(i);

        for (final Integer child : children) {
            if (isCyclicUtil(child, visited, stack)) {
                return true;
            }
        }

        stack[i] = false;

        return false;
    }

    private List<Integer> getChildren(final Integer id) {
        return pipes.stream()
                .filter(pipe -> pipe.getInput().equals(id))
                .map(PipeDefinition::getOutput)
                .collect(Collectors.toList());
    }
}
