package org.bricolages.streaming.stream;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.bricolages.streaming.exception.ConfigError;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Autowired;
import javax.persistence.*;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class StreamRouter {
    final List<Entry> entries;

    void check() throws ConfigError {
        for (Entry ent : entries) {
            try {
                ent.sourcePattern();
            }
            catch (PatternSyntaxException ex) {
                throw new ConfigError("source pattern syntax error: " + ent.srcUrlPattern);
            }
        }
    }

    @Autowired
    DataPacketRepository packetRepos;

    public DataPacket loadPacket(long packetId) {
        return packetRepos.findOne(packetId);
    }

    public DataPacket route(S3ObjectLocation src) throws ConfigError {
        val result = parseUrl(src);
        if (result == null) return null;
        DataStream stream = findOrCreateStream(result.streamName, result.streamPrefix);
        return new DataPacket(stream, src, result.destLocation());
    }

    @Autowired
    DataStreamRepository streamRepos;

    DataStream findOrCreateStream(String name, String prefix) {
        DataStream stream = streamRepos.findStream(name);
        if (stream != null) {
            return stream;
        }
        else {
            return createStream(name, prefix);
        }
    }

    @Bean
    DataStream createStream(String name, String prefix) {
        log.warn("received new stream: {}", name);
        DataStream stream = new DataStream(name, prefix);
        streamRepos.save(stream); // FIXME: ignore duplicated error
        return stream;
    }

    ParseResult parseUrl(S3ObjectLocation src) throws ConfigError {
        for (Entry ent : entries) {
            Matcher m = ent.sourcePattern().matcher(src.urlString());
            if (m.matches()) {
                return new ParseResult(
                    safeSubst(ent.streamName, m),
                    ent.destBucket,
                    safeSubst(ent.streamPrefix, m),
                    safeSubst(ent.objectPrefix, m),
                    safeSubst(ent.objectName, m)
                );
            }
        }
        // FIXME: avoid to send too many logs
        log.error("could not map the object URL to any stream: {}", src);
        return null;
    }

    String safeSubst(String template, Matcher m) {
        try {
            return m.replaceFirst(template);
        }
        catch (IndexOutOfBoundsException ex) {
            throw new ConfigError("bad replacement: " + template);
        }
    }

    @NoArgsConstructor
    public static final class Entry {
        public String srcUrlPattern;
        public String streamName;
        public String destBucket;
        public String streamPrefix;
        public String objectPrefix;
        public String objectName;

        Entry(String srcUrlPattern, String streamName, String destBucket, String streamPrefix, String objectPrefix, String objectName) {
            this.srcUrlPattern = srcUrlPattern;
            this.streamName = streamName;
            this.destBucket = destBucket;
            this.streamPrefix = streamPrefix;
            this.objectPrefix = objectPrefix;
            this.objectName = objectName;
        }

        Pattern pat = null;

        Pattern sourcePattern() {
            if (pat != null) return pat;
            pat = Pattern.compile("^" + srcUrlPattern + "$");
            return pat;
        }
    }

    @RequiredArgsConstructor
    static final class ParseResult {
        final String streamName;
        final String destBucket;
        final String streamPrefix;
        final String objectPrefix;
        final String objectName;

        S3ObjectLocation destLocation() {
            return new S3ObjectLocation(destBucket, streamPrefix + "/" + objectPrefix + "/" + objectName);
        }
    }
}
