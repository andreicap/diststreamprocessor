package it.polimi.operator;

import lombok.extern.java.Log;

import java.io.IOException;
import java.net.Socket;

@Log
public class Operator {

    public void register() {
        log.info("Registering operator");
        try {
            Socket socket = new Socket("localhost", 2000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
