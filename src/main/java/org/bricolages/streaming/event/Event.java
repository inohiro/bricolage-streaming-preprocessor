package org.bricolages.streaming.event;
import com.amazonaws.services.sqs.model.Message;
import java.util.stream.Stream;
import java.util.List;
import java.util.ArrayList;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public abstract class Event {
    static final List<MessageParser> PARSERS = new ArrayList<MessageParser>();
    static {
        PARSERS.add(new S3Event.Parser());
        PARSERS.add(new ProcessPacketEvent.Parser());
        PARSERS.add(new FlushPendingEvent.Parser());
        PARSERS.add(new ShutdownEvent.Parser());
        PARSERS.add(new UnknownEvent.Parser());
    }

    static public Stream<Event> streamForMessage(Message msg) {
        try {
            for (MessageParser parser : PARSERS) {
                if (parser.isCompatible(msg)) {
                    Event e = parser.parse(msg);
                    return (e == null) ? Stream.empty() : Stream.of(e);
                }
            }
            return Stream.of(new UnknownEvent(msg));
        }
        catch (MessageParseException ex) {
            // FIXME: throttle
            log.error("SQS message body parse error: {}", ex.getMessage());
            return Stream.of(new UnknownEvent(msg));
        }
    }

    final Message message;

    public String getMessageId() {
        return message.getMessageId();
    }

    public String getMessageBody() {
        return message.getBody();
    }

    public String getReceiptHandle() {
        return message.getReceiptHandle();
    }

    public abstract void callHandler(EventHandlers h);

    @Override
    public String toString() {
        return "#<Event messageId=" + getMessageId() + ">";
    }

    // default implementation
    public String getBody() {
        if (message == null) {
            throw new UnsupportedOperationException("Event#getBody()");
        }
        return message.getBody();
    }
}
