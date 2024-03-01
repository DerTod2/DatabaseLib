package net.dertod2.DatabaseLib.Exceptions;

import java.io.Serial;

public class UnsupportedMapDepthException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 8703424388584694879L;

    @Override
    public String getMessage() {
        return "Maps are only supported with an maximum depth of one";
    }
}
