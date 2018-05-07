package it.polimi.supervisor;

import lombok.Value;
import lombok.extern.java.Log;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Log
@Component
class Logger {

    private final SimpMessagingTemplate template;

    @Autowired
    Logger(final SimpMessagingTemplate template) {
        this.template = template;
    }

    void info(final String message) {
        log.info(message);
        log("INFO", message);
    }

    void warn(final String message) {
        log.warning(message);
        log("WARN", message);
    }

    void severe(final String message) {
        log.severe(message);
        log("SEVERE", message);
    }

    private void log(final String level, final String message) {
        template.convertAndSend("/topic/logs", new Log(level, message));
    }

    @Value
    private class Log {
        private final String time = DateTime.now().toString();

        private final String level;

        private final String message;
    }
}
