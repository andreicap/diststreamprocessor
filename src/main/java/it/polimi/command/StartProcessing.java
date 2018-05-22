package it.polimi.command;

import it.polimi.OperatorNode;
import it.polimi.supervisor.graph.Aggregation;
import it.polimi.supervisor.worker.State;
import it.polimi.util.AggregationFunc;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import java.io.IOException;

@Log
@RequiredArgsConstructor
public class StartProcessing implements Command {

    private final Integer operatorId;

    private final Integer windowSize;

    private final Integer windowSlide;

    private final Aggregation aggregation;

    private final Integer inputsNumber;

    @Override
    public Object execute() {
        OperatorNode.state = State.BUSY;

        while (emptyInputStreams() < inputsNumber) {
            OperatorNode.inputStreams
                    .forEach(inputStream -> AggregationFunc.getFunc(aggregation)
                            .apply(inputStream.getWindow(windowSize, windowSlide))
                            .ifPresent(this::forwardResult));
        }

        cleanUp();
        OperatorNode.state = State.FREE;
        return OperatorNode.state;
    }

    private void forwardResult(double result) {
        OperatorNode.outputStreams.forEach(outputStream -> {
            try {
                outputStream.writeDouble(result);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private long emptyInputStreams() {
        return OperatorNode.inputStreams.stream()
                .filter(inputStream -> inputStream.isEmpty(windowSize))
                .count();
    }

    private void cleanUp() {
        OperatorNode.inputStreams.clear();
        OperatorNode.outputStreams.forEach(outputStream -> {
            try {
                outputStream.flush();
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        OperatorNode.outputStreams = null;
    }
}
