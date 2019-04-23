package collection.implementation;


import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class MeetupWebSocketHandler extends AbstractWebSocketHandler {

    private static final Logger logger =
            Logger.getLogger(MeetupWebSocketHandler.class.getName());

    private final MeetupKafkaProducer meetupKafkaProducer;

    public MeetupWebSocketHandler(MeetupKafkaProducer meetupKafkaProducer) {
        this.meetupKafkaProducer = meetupKafkaProducer;
    }

    @Override
    public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) {
        logger.log(Level.INFO, "New RSVP:\n {0}", message.getPayload());

        meetupKafkaProducer.sendRsvpMessage(message);
    }
}
