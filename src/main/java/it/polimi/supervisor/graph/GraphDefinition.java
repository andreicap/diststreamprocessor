package it.polimi.supervisor.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.java.Log;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.List;

@Data
@Log
public class GraphDefinition {

    private List<OperatorDefinition> operators;

    private List<PipeDefinition> pipes;

    public static GraphDefinition fromString(final String json) throws IOException {
        return new ObjectMapper().readValue(json, GraphDefinition.class);
    }

    public Boolean validateGraph() {
        log.info("Graph Validation Started");
//        System.out.println(operators);
//        System.out.println(pipes);
        Boolean isCyclic = isCyclic();
        log.info("Is cyclic? - " + isCyclic);


        return true;
    }


    private Boolean isCyclic() {

        // Mark all the vertices as not visited and
        // not part of recursion stack
        int V = operators.size();
        boolean[] visited = new boolean[V];
        boolean[] stack = new boolean[V];

        // Call the recursive helper function to
        // detect cycle in different DFS trees
        for (int i = 0; i < V; i++)
            if (isCyclicUtil(i, visited, stack))
                return true;

        return false;
    }

    private boolean isCyclicUtil(int i, boolean[] visited, boolean[] stack) {

        if (stack[i])
            return true;

        if (visited[i])
            return false;

        visited[i] = true;

        stack[i] = true;
        List<Integer> children = getChildren(i);

        for (Integer c : children)
            if (isCyclicUtil(c, visited, stack))
                return true;

        stack[i] = false;

        return false;
    }

    private List<Integer> getChildren(Integer id) {

        List<Integer> children = new ArrayList<Integer>(operators.size());
        int i = 0;
        for (PipeDefinition p : pipes) {
            if (p.getInput() == i) {
                children.add(p.getOutput().intValue());
            }
            i++;
        }
        return children;
    }

}