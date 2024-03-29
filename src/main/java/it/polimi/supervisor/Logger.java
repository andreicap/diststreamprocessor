package it.polimi.supervisor;

import lombok.Value;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

@Log
@Component
public class Logger {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final SimpMessagingTemplate template;

    @Autowired
    public Logger(final SimpMessagingTemplate template) {
        this.template = template;
    }

    public void info(final String message) {
        log.info(message);
        log("INFO", message);
    }

    public void warn(final String message) {
        log.warning(message);
        log("WARN", message);
    }

    public void severe(final String message) {
        log.severe(message);
        log("SEVERE", message);
    }

    private void log(final String level, final String message) {
        template.convertAndSend("/topic/logs", new Log(level, message));
    }

    @Value
    private class Log {
        private final String time = DATE_FORMAT.format(new Date());

        private final String level;

        private final String message;
    }
}
