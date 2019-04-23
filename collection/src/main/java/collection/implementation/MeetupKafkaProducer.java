package collection.implementation;


import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketMessage;

@Component
@EnableBinding(Source.class)
public class MeetupKafkaProducer {

    private static final int MESSAGE_TIMEOUT_MS = 10000;

    private final Source source;

    public MeetupKafkaProducer(Source source) {
        this.source = source;
    }

    protected void sendRsvpMessage(WebSocketMessage<?> message) {
        source.output()
                .send(MessageBuilder.withPayload(message.getPayload())
                        .build(), MESSAGE_TIMEOUT_MS);
    }
}
