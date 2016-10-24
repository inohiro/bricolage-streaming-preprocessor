package org.bricolages.streaming.stream;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.bricolages.streaming.s3.S3UrlParseException;
import org.bricolages.streaming.exception.ConfigError;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class StreamRouterTest {
    StreamRouter newRouter(StreamRouter.Entry... entries) {
        return new StreamRouter(Arrays.asList(entries));
    }

    StreamRouter.Entry entry(String src, String stream, String destBucket, String streamPrefix, String objectPrefix, String objectName) {
        return new StreamRouter.Entry(src, stream, destBucket, streamPrefix, objectPrefix, objectName);
    }

    S3ObjectLocation loc(String url) throws S3UrlParseException {
        return S3ObjectLocation.forUrl(url);
    }

    @Test
    public void route() throws Exception {
        StreamRouter router = newRouter(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "dest-bucket", "dest-prefix/$1", "YMD", "$2"));
        router.check();
        val result = router.parseUrl(loc("s3://src-bucket/src-prefix/schema.table/datafile.json.gz"));
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/YMD/datafile.json.gz"), result.destLocation());
        assertEquals("schema.table", result.streamName);
        assertNull(router.parseUrl(loc("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz")));
    }

    @Test(expected=ConfigError.class)
    public void route_bad_destination() throws Exception {
        StreamRouter router = newRouter(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$5", "dest-bucket", "dest-prefix", "YMD", "$2"));
        router.check();
        router.parseUrl(loc("s3://src-bucket/src-prefix/schema.table/datafile.json.gz"));
    }

    @Test(expected=ConfigError.class)
    public void route_bad_regexp() throws Exception {
        StreamRouter router = newRouter(entry("****", "$1", "dest-bucket", "dest-prefix/$2", "YMD", "$3"));
        router.check();
    }
}
