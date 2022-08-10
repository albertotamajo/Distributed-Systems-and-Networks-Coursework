/*
 * Decompiled with CFR 0.150.
 */
import java.io.IOException;

public class FileAlreadyExistsException
extends IOException {
    private static final long serialVersionUID = -2050950133394985954L;

    /*
     * WARNING - void declaration
     */
    public FileAlreadyExistsException(String filename) {
        super("Error trying to store file " + (String)var1_1 + " - file already exists");
        void var1_1;
    }
}

