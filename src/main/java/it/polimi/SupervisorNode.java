package it.polimi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("it.polimi.supervisor")
public class SupervisorNode {

    public static void main(String[] args) {
        SpringApplication.run(SupervisorNode.class, args);
    }
}
