package collection.implementation;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

@SpringBootApplication
public class MeetRSVPStreamingApplication {

    private static final String RSVP_ENDPOINT = "ws://stream.meetup.com/2/rsvps";

    public static void main(String[] args) {
        SpringApplication.run(MeetRSVPStreamingApplication.class, args);
    }

    @Bean
    public ApplicationRunner initializeConnection(MeetupWebSocketHandler handler) {
        return args -> {
            WebSocketClient webSocketClient = new StandardWebSocketClient();

            webSocketClient.doHandshake(handler, RSVP_ENDPOINT);
        };
    }


}
