package it.polimi.supervisor;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.net.Socket;

@Getter
@AllArgsConstructor
class Worker {

    private final Long workerId;

    private final Socket socket;

    WorkerDetails getWorkerDetails() {
        return new WorkerDetails()
                .withWorkerId(workerId)
                .withHost(socket.getInetAddress().getHostName())
                .withPort(socket.getPort());
        // TODO check health and get some OS statistics
    }
}
