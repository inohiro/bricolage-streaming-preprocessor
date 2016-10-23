package org.bricolages.streaming.event;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.io.IOException;
import lombok.*;

public class ProcessPacketEvent extends Event {
    static final class Parser implements MessageParser {
        @Override
        public boolean isCompatible(Message msg) {
            return msg.getBody().contains("\"eventName\":\"processPacket");
        }

        static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public Event parse(Message msg) throws MessageParseException {
            try {
                MessageBody body = MAPPER.readValue(msg.getBody(), MessageBody.class);
                val rec = body.records.get(0);
                return new ProcessPacketEvent(msg, rec.packetId, rec.noDispatch);
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
        List<Record> records;
    }

    static final class Record {
        long packetId;
        boolean noDispatch;
    }

    @Getter
    final long packetId;

    final boolean noDispatch;

    ProcessPacketEvent(Message msg, long packetId, boolean noDispatch) {
        super(msg);
        this.packetId = packetId;
        this.noDispatch = noDispatch;
    }

    public ProcessPacketEvent(long packetId, boolean noDispatch) {
        this(null, packetId, noDispatch);
    }

    public boolean doesNotDispatch() {
        return noDispatch;
    }

    public void callHandler(EventHandlers h) {
        h.handleProcessPacketEvent(this);
    }

    @Override
    public String toString() {
        return "#<ProcessPacketEvent messageId=" + getMessageId() + " packetId=" + packetId + ">";
    }

    @Override
    public String getBody() {
        val w = new SQSMessageBodyWriter();
        w.beginObject();
            w.beginArray("Records");
                w.beginObject();
                    w.pair("eventSource", "bricolage:preprocessor");
                    w.pair("eventName", "processPacket");
                    w.pair("packetId", packetId);
                    w.pair("noDispatch", noDispatch);
                w.endObject();
            w.endArray();
        w.endObject();
        return w.toString();
    }
}
