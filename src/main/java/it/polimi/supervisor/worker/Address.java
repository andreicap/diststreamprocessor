package it.polimi.supervisor.worker;

import lombok.Value;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Optional;

@Value
public class Address implements Serializable {
    private final String host;

    private final Integer port;

    public Optional<ObjectOutputStream> getOptionalObjectOutputStream() {
        try {
            final Socket socket = new Socket(getHostOrLocalhostIfOnSameMachine(), port);
            final ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
            return Optional.of(outputStream);
        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    public String getHostOrLocalhostIfOnSameMachine() {
        try {
            if (host.equals(InetAddress.getLocalHost().getHostAddress())) {
                return "localhost";
            }
            return host;
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
