package it.polimi;

import it.polimi.command.Command;
import lombok.extern.java.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;

@Log
public class OperatorNode {

    private static ObjectInputStream input;

    private static ObjectOutputStream output;

    public static void main(String[] args) {
        register();
    }

    private static void register() {
        log.info("Registering operator");
        try {
            final Socket socket = new Socket("localhost", 2000);
            input = new ObjectInputStream(socket.getInputStream());
            output = new ObjectOutputStream(socket.getOutputStream());

            new Thread(OperatorNode::serveCommands).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void serveCommands() {
        try {
            while (true) {
                Object command = input.readObject();
                if (command instanceof Command) {
                    log.info("Executing command: " + command);
                    asyncExecuteCommand((Command) command);
                } else {
                    log.warning("Unknown message: " + command);
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void asyncExecuteCommand(final Command command) {
        CompletableFuture
                .supplyAsync(command::execute)
                .thenAccept(OperatorNode::sendResult);
    }

    private static void sendResult(final Object result) {
        try {
            log.info("Sending result: " + result);
            output.writeObject(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
