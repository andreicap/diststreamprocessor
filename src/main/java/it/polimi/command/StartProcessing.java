package it.polimi.command;

import it.polimi.OperatorNode;
import it.polimi.supervisor.graph.Aggregation;
import it.polimi.supervisor.worker.State;
import it.polimi.util.AggregationFunc;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

@Log
@RequiredArgsConstructor
public class StartProcessing implements Command {

    private final Integer operatorId;

    private final Integer windowSize;

    private final Integer windowSlide;

    private final Aggregation aggregation;

    private final Integer inputsNumber;

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public Object execute() {
        OperatorNode.state = State.BUSY;

        await().until(() -> OperatorNode.inputWindowStreams.size() == inputsNumber);

        OperatorNode.inputWindowStreams
                .forEach(inputWindowStream -> inputWindowStream
                        .withWindowSize(windowSize)
                        .withWindowSlide(windowSlide)
                        .onEndOfStream(counter::incrementAndGet)
                        .subscribe((window) ->
                                AggregationFunc.getFunc(aggregation)
                                        .apply(window.stream())
                                        .ifPresent(this::forwardResult)
                        )
                        .start());

        await().until(() -> counter.get() == inputsNumber);

        cleanUp();
        OperatorNode.state = State.FREE;
        return OperatorNode.state;
    }

    private void forwardResult(final Double result) {
        OperatorNode.outputStreams.forEach(outputStream -> {
            try {
                outputStream.writeObject(result);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void cleanUp() {
        OperatorNode.inputWindowStreams.clear();
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
