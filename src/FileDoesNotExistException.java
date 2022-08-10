/*
 * Decompiled with CFR 0.150.
 */
import java.io.IOException;

public class FileDoesNotExistException
extends IOException {
    private static final long serialVersionUID = 3234056968693564846L;

    /*
     * WARNING - void declaration
     */
    public FileDoesNotExistException(String filename) {
        super("Error trying to load or remove file " + (String)var1_1 + " - file does not exist");
        void var1_1;
    }
}

