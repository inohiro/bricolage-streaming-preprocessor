package org.bricolages.streaming.event;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.io.IOException;
import lombok.*;

public class FlushPendingEvent extends Event {
    static final class Parser implements MessageParser {
        @Override
        public boolean isCompatible(Message msg) {
            return msg.getBody().contains("\"eventName\":\"flushPending");
        }

        static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public Event parse(Message msg) throws MessageParseException {
            try {
                MessageBody body = MAPPER.readValue(msg.getBody(), MessageBody.class);
                return new FlushPendingEvent(msg, body.Records.get(0).streamId);
            }
            catch (JsonProcessingException ex) {
                throw new MessageParseException(ex.getMessage());
            }
            catch (IOException ex) {
                // FIXME?
                throw new MessageParseException(ex.getMessage());
            }
        }
    }

    static final class MessageBody {
        List<Record> Records;
    }

    static final class Record {
        int streamId;
    }

    @Getter
    final int streamId;

    FlushPendingEvent(Message msg, int streamId) {
        super(msg);
        this.streamId = streamId;
    }

    public void callHandler(EventHandlers h) {
        h.handleFlushPendingEvent(this);
    }

    @Override
    public String toString() {
        return "#<FlushPendingEvent messageId=" + getMessageId() + " streamId=" + streamId + ">";
    }

    @Override
    public String getBody() {
        val w = new SQSMessageBodyWriter();
        w.beginObject();
            w.beginArray("Records");
                w.beginObject();
                    w.pair("eventSource", "bricolage:preprocessor");
                    w.pair("eventName", "flushPending");
                    w.pair("streamId", streamId);
                w.endObject();
            w.endArray();
        w.endObject();
        return w.toString();
    }
}
