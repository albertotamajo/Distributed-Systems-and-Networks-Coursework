/*
 * Decompiled with CFR 0.150.
 */
public class ClientLogger
extends Logger {
    private static ClientLogger a = null;

    public static synchronized void init(LoggingType loggingType) {
        if (a == null) {
            LoggingType loggingType2;
            a = new ClientLogger(loggingType2);
        }
    }

    public static ClientLogger getInstance() {
        if (a == null) {
            throw new RuntimeException("ClientLogger has not been initialised yet");
        }
        return a;
    }

    /*
     * WARNING - void declaration
     */
    protected ClientLogger(LoggingType loggingType) {
        super((LoggingType)var1_1);
        void var1_1;
    }

    @Override
    protected String getLogFileSuffix() {
        return "client";
    }

    /*
     * WARNING - void declaration
     */
    public void errorConnecting(int cport) {
        void var1_1;
        this.log("Cannot connect to the Controller on port ".concat(String.valueOf((int)var1_1)));
    }

    /*
     * WARNING - void declaration
     */
    public void storeStarted(String filename) {
        void var1_1;
        this.log("Store operation started for file ".concat(String.valueOf(var1_1)));
    }

    /*
     * WARNING - void declaration
     */
    public void dstoresWhereToStoreTo(String filename, int[] dstorePorts) {
        void var2_2;
        StringBuffer stringBuffer;
        stringBuffer = new StringBuffer("Controller replied to store " + (String)((Object)stringBuffer) + " in these Dstores: ");
        for (void var2_3 : var2_2) {
            stringBuffer.append(String.valueOf((int)var2_3) + " ");
        }
        this.log(stringBuffer.toString());
    }

    /*
     * WARNING - void declaration
     */
    public void storeToDstoreStarted(String filename, int dstorePort) {
        void var2_2;
        void var1_1;
        this.log("Storing file " + (String)var1_1 + " to Dstore " + (int)var2_2);
    }

    /*
     * WARNING - void declaration
     */
    public void ackFromDstore(String filename, int dstorePort) {
        void var1_1;
        void var2_2;
        this.log("ACK received from Dstore " + (int)var2_2 + " to store file " + (String)var1_1);
    }

    /*
     * WARNING - void declaration
     */
    public void storeToDstoreCompleted(String filename, int dstorePort) {
        void var2_2;
        void var1_1;
        this.log("Store of file " + (String)var1_1 + " to Dstore " + (int)var2_2 + " successfully completed");
    }

    /*
     * WARNING - void declaration
     */
    public void storeToDstoreFailed(String filename, int dstorePort) {
        void var2_2;
        void var1_1;
        this.log("Store of file " + (String)var1_1 + " to Dstore " + (int)var2_2 + " failed");
    }

    /*
     * WARNING - void declaration
     */
    public void fileToStoreAlreadyExists(String filename) {
        void var1_1;
        this.log("File to store " + (String)var1_1 + " already exists in the data store");
    }

    /*
     * WARNING - void declaration
     */
    public void storeCompleted(String filename) {
        void var1_1;
        this.log("Store operation for file " + (String)var1_1 + " completed");
    }

    /*
     * WARNING - void declaration
     */
    public void loadStarted(String filename) {
        void var1_1;
        this.log("Load operation for file " + (String)var1_1 + " started");
    }

    /*
     * WARNING - void declaration
     */
    public void retryLoad(String filename) {
        void var1_1;
        this.log("Retrying to load file ".concat(String.valueOf(var1_1)));
    }

    /*
     * WARNING - void declaration
     */
    public void dstoreWhereToLoadFrom(String filename, int dstorePort, int filesize) {
        void var2_2;
        void var3_3;
        void var1_1;
        this.log("Controller replied to load file " + (String)var1_1 + " (size: " + (int)var3_3 + " bytes) from Dstore " + (int)var2_2);
    }

    /*
     * WARNING - void declaration
     */
    public void loadFromDstore(String filename, int dstorePort) {
        void var2_2;
        void var1_1;
        this.log("Loading file " + (String)var1_1 + " from Dstore " + (int)var2_2);
    }

    /*
     * WARNING - void declaration
     */
    public void loadFromDstoreFailed(String filename, int dstorePort) {
        void var2_2;
        void var1_1;
        this.log("Load operation for file " + (String)var1_1 + " from Dstore " + (int)var2_2 + " failed");
    }

    /*
     * WARNING - void declaration
     */
    public void loadFailed(String filename, int dstoreCount) {
        void var2_2;
        void var1_1;
        this.log("Load operation for file " + (String)var1_1 + " failed after having contacted " + (int)var2_2 + " different Dstores");
    }

    /*
     * WARNING - void declaration
     */
    public void fileToLoadDoesNotExist(String filename) {
        void var1_1;
        this.log("Load operation failed because file does not exist (filename: " + (String)var1_1 + ")");
    }

    /*
     * WARNING - void declaration
     */
    public void loadCompleted(String filename, int dstoreCount) {
        void var2_2;
        void var1_1;
        this.log("Load operation of file " + (String)var1_1 + " from Dstore " + (int)var2_2 + " successfully completed");
    }

    /*
     * WARNING - void declaration
     */
    public void removeStarted(String filename) {
        void var1_1;
        this.log("Remove operation for file " + (String)var1_1 + " started");
    }

    /*
     * WARNING - void declaration
     */
    public void fileToRemoveDoesNotExist(String filename) {
        void var1_1;
        this.log("Remove operation failed because file does not exist (filename: " + (String)var1_1 + ")");
    }

    /*
     * WARNING - void declaration
     */
    public void removeComplete(String filename) {
        void var1_1;
        this.log("Remove operation for file " + (String)var1_1 + " successfully completed");
    }

    /*
     * WARNING - void declaration
     */
    public void removeFailed(String filename) {
        void var1_1;
        this.log("Remove operation for file " + (String)var1_1 + " not completed successfully");
    }

    public void listStarted() {
        this.log("List operation started");
    }

    public void listFailed() {
        this.log("List operation failed");
    }

    public void listCompleted() {
        this.log("List operation successfully completed");
    }
}

