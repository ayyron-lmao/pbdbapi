package net.playblack.pbdataaccess.exceptions;

public class DatabaseWriteException extends Exception {

    private static final long serialVersionUID = -6274875008612771399L;

    public DatabaseWriteException(String str) {
        super(str);
    }

    public DatabaseWriteException(String str, Throwable t) {
        super(str, t);
    }
}
