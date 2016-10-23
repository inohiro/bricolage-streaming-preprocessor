package org.bricolages.streaming.filter;
import org.bricolages.streaming.exception.ApplicationException;

public class JSONException extends ApplicationException {
    public JSONException(String message) {
        super(message);
    }

    public JSONException(Exception cause) {
        super(cause);
    }
}
