package it.polimi;

import com.google.common.collect.Lists;
import it.polimi.command.Command;
import it.polimi.supervisor.worker.Address;
import it.polimi.supervisor.worker.State;
import it.polimi.util.WindowedInputStream;
import lombok.extern.java.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

@Log
public class OperatorNode {

    private static final int ANY_PORT = 0;

    public static List<WindowedInputStream> inputStreams = Lists.newArrayList();

    public static List<ObjectOutputStream> outputStreams;

    public static State state = State.FREE;

    private static ServerSocket serverSocket;

    private static ObjectInputStream supervisorInputStream;

    private static ObjectOutputStream supervisorOutputStream;

    public static void main(String[] args) {
        new Thread(OperatorNode::acceptInputStreams).start();
        register();
        new Thread(OperatorNode::serveSupervisorCommands).start();
    }

    private static void acceptInputStreams() {
        try {
            serverSocket = new ServerSocket(ANY_PORT);
            log.info("Listening for input on: " + serverSocket);
            while (true) {
                final Socket clientSocket = serverSocket.accept();
                log.info("Received connection from: " + clientSocket);
                inputStreams.add(new WindowedInputStream(clientSocket));
            }
        } catch (IOException e) {
            log.severe("Operator failed.");
            e.printStackTrace();
        }
    }

    private static void register() {
        try {
            log.info("Registering operator.");
            final Socket socket = new Socket("localhost", 2000);
            supervisorOutputStream = new ObjectOutputStream(socket.getOutputStream());
            final Address address = new Address(serverSocket.getInetAddress().getHostName(), serverSocket.getLocalPort());
            supervisorOutputStream.writeObject(address);
            supervisorInputStream = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            log.severe("Failed to register.");
            e.printStackTrace();
        }
    }

    private static void serveSupervisorCommands() {
        try {
            while (true) {
                Object object = supervisorInputStream.readObject();
                if (object instanceof Command) {
                    final Command command = (Command) object;
                    log.info("Executing command: " + command);
                    asyncExecuteCommand(command);
                } else {
                    log.warning("Unknown message: " + object);
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void asyncExecuteCommand(final Command command) {
        new Thread(() -> sendResult(command.execute())).start();
    }

    private static void sendResult(final Object result) {
        if (result == null) {
            return;
        }

        try {
            log.info("Sending result: " + result);
            supervisorOutputStream.writeObject(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
