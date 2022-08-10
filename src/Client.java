/*
 * Decompiled with CFR 0.150.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class Client {
    private final int a;
    private final int b;
    private Socket c;
    private BufferedReader d;
    private PrintWriter e;
    private int f;

    /*
     * WARNING - void declaration
     */
    public Client(int cport, int timeout, Logger.LoggingType loggintType) {
        void var3_3;
        void var2_2;
        void var1_1;
        this.a = var1_1;
        this.b = var2_2;
        ClientLogger.init((Logger.LoggingType)var3_3);
    }

    public void connect() throws IOException {
        try {
            this.disconnect();
        }
        catch (IOException iOException) {}
        try {
            this.c = new Socket(InetAddress.getLoopbackAddress(), this.a);
            this.c.setSoTimeout(this.b);
            ClientLogger.getInstance().connectionEstablished(this.c.getPort());
            this.e = new PrintWriter(this.c.getOutputStream(), true);
            this.d = new BufferedReader(new InputStreamReader(this.c.getInputStream()));
            return;
        }
        catch (Exception exception) {
            ClientLogger.getInstance().errorConnecting(this.a);
            throw exception;
        }
    }

    public void disconnect() throws IOException {
        if (this.c != null) {
            this.c.close();
        }
    }

    /*
     * WARNING - void declaration
     */
    public void send(String message) {
        void var1_1;
        this.e.println(message);
        ClientLogger.getInstance().messageSent(this.c.getPort(), (String)var1_1);
    }

    public String[] list() throws IOException, NotEnoughDstoresException {
        String[] arrstring;
        this.e.println("LIST");
        ClientLogger.getInstance().messageSent(this.c.getPort(), "LIST");
        ClientLogger.getInstance().listStarted();
        try {
            arrstring = this.d.readLine();
        }
        catch (SocketTimeoutException socketTimeoutException) {
            ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
            ClientLogger.getInstance().listFailed();
            throw socketTimeoutException;
        }
        ClientLogger.getInstance().messageReceived(this.c.getPort(), (String)arrstring);
        if (arrstring != null && (arrstring = arrstring.split(" ")).length > 0) {
            if (arrstring[0].equals("ERROR_NOT_ENOUGH_DSTORES")) {
                ClientLogger.getInstance().error("Not enough Dstores have joined the data store yet");
                ClientLogger.getInstance().listFailed();
                throw new NotEnoughDstoresException();
            }
            if (arrstring[0].equals("LIST")) {
                String[] arrstring2 = new String[arrstring.length - 1];
                for (int i = 0; i < arrstring2.length; ++i) {
                    arrstring2[i] = arrstring[i + 1];
                }
                ClientLogger.getInstance().listCompleted();
                return arrstring2;
            }
        }
        arrstring = "Connection closed by the Controller";
        ClientLogger.getInstance().error((String)arrstring);
        ClientLogger.getInstance().listFailed();
        throw new IOException((String)arrstring);
    }

    /*
     * WARNING - void declaration
     */
    public void store(File file) throws IOException, NotEnoughDstoresException, FileAlreadyExistsException {
        byte[] arrby;
        if (!file.exists()) {
            String string = "File to store does not exist (absolute path: " + file.getAbsolutePath() + ")";
            ClientLogger.getInstance().error(string);
            throw new IOException(string);
        }
        String string = file.getName();
        if (string.contains(" ")) {
            String string2 = "Filename includes spaces (absolute path: " + file.getAbsolutePath() + ")";
            ClientLogger.getInstance().error(string2);
            throw new IOException(string2);
        }
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            arrby = fileInputStream.readAllBytes();
            fileInputStream.close();
        }
        catch (IOException iOException) {
            void var1_1;
            ClientLogger.getInstance().error("Error reading data from file (absolute path: " + var1_1.getAbsolutePath() + ")");
            throw iOException;
        }
        this.store(string, arrby);
    }

    /*
     * WARNING - void declaration
     */
    public void store(String filename2, byte[] data) throws IOException, NotEnoughDstoresException, FileAlreadyExistsException {
        String string;
        String string2;
        String string3 = "STORE " + filename2 + " " + data.length;
        this.e.println(string3);
        ClientLogger.getInstance().messageSent(this.c.getPort(), string3);
        ClientLogger.getInstance().storeStarted(filename2);
        try {
            string2 = this.d.readLine();
        }
        catch (SocketTimeoutException socketTimeoutException) {
            ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
            throw socketTimeoutException;
        }
        ClientLogger.getInstance().messageReceived(this.c.getPort(), string2);
        int[] arrn = Client.a(filename2, string2);
        ClientLogger.getInstance().dstoresWhereToStoreTo(filename2, arrn);
        int[] arrn2 = arrn;
        int n = arrn.length;
        for (int i = 0; i < n; ++i) {
            int n2 = arrn2[i];
            Socket socket = null;
            try {
                try {
                    socket = new Socket(InetAddress.getLoopbackAddress(), n2);
                    ClientLogger.getInstance().connectionEstablished(socket.getPort());
                    Object object = socket.getOutputStream();
                    Object object2 = new PrintWriter((OutputStream)object, true);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    ((PrintWriter)object2).println(string3);
                    ClientLogger.getInstance().messageSent(socket.getPort(), string3);
                    ClientLogger.getInstance().storeToDstoreStarted(filename2, n2);
                    try {
                        object2 = bufferedReader.readLine();
                    }
                    catch (SocketTimeoutException socketTimeoutException) {
                        ClientLogger.getInstance().timeoutExpiredWhileReading(socket.getPort());
                        throw socketTimeoutException;
                    }
                    ClientLogger.getInstance().messageReceived(socket.getPort(), (String)object2);
                    if (object2 == null) {
                        object = "Connection closed by Dstore ".concat(String.valueOf(n2));
                        ClientLogger.getInstance().error((String)object);
                        throw new IOException((String)object);
                    }
                    if (!((String)object2).trim().equals("ACK")) {
                        object = "Unexpected message received from Dstore (ACK was expected): ".concat(String.valueOf(object2));
                        ClientLogger.getInstance().error((String)object);
                        throw new IOException((String)object);
                    }
                    ClientLogger.getInstance().ackFromDstore(filename2, n2);
                    ((OutputStream)object).write(data);
                    ClientLogger.getInstance().storeToDstoreCompleted(filename2, n2);
                }
                catch (Exception exception) {
                    ClientLogger.getInstance().storeToDstoreFailed(filename2, n2);
                    if (socket == null) continue;
                }
            }
            catch (Throwable filename2) {
                if (socket != null) {
                    socket.close();
                }
                throw filename2;
            }
            socket.close();
        }
        try {
            string = this.d.readLine();
        }
        catch (SocketTimeoutException socketTimeoutException) {
            ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
            throw socketTimeoutException;
        }
        ClientLogger.getInstance().messageReceived(this.c.getPort(), string);
        if (string == null) {
            String string4 = "Connection closed by the Controller";
            ClientLogger.getInstance().error(string4);
            throw new IOException(string4);
        }
        if (string.trim().equals("STORE_COMPLETE")) {
            void var1_1;
            ClientLogger.getInstance().storeCompleted((String)var1_1);
            return;
        }
        String string5 = "Unexpected message received (STORE_COMPLETE was expected): ".concat(String.valueOf(string2));
        ClientLogger.getInstance().error(string5);
        throw new IOException(string5);
    }

    private static int[] a(String object, String string) throws IOException {
        if (string == null) {
            object = "Connection closed by the Controller";
            ClientLogger.getInstance().error((String)object);
            throw new IOException((String)object);
        }
        String[] arrstring = string.split(" ");
        if (arrstring[0].equals("STORE_TO")) {
            object = new int[arrstring.length - 1];
            for (int i = 0; i < ((Object)object).length; ++i) {
                object[i] = Integer.parseInt(arrstring[i + 1]);
            }
        } else {
            if (arrstring[0].equals("ERROR_FILE_ALREADY_EXISTS")) {
                ClientLogger.getInstance().fileToStoreAlreadyExists((String)object);
                throw new FileAlreadyExistsException((String)object);
            }
            if (arrstring[0].equals("ERROR_NOT_ENOUGH_DSTORES")) {
                ClientLogger.getInstance().error("Not enough Dstores have joined the data store yet");
                throw new NotEnoughDstoresException();
            }
            string = "Unexpected message received: ".concat(String.valueOf(string));
            ClientLogger.getInstance().error(string);
            throw new IOException(string);
        }
        return object;
    }

    /*
     * WARNING - void declaration
     */
    public void load(String filename, File fileFolder) throws IOException, NotEnoughDstoresException, FileDoesNotExistException {
        Object object;
        void var2_2;
        if (!fileFolder.exists()) {
            String string = "The folder where to store the file does not exist (absolute path: " + fileFolder.getAbsolutePath() + ")";
            ClientLogger.getInstance().error(string);
            throw new IOException(string);
        }
        if (!fileFolder.isDirectory()) {
            String string = "The provided folder where to store the file is not actually a directory (absolute path: " + fileFolder.getAbsolutePath() + ")";
            ClientLogger.getInstance().error(string);
            throw new IOException(string);
        }
        byte[] arrby = this.load(filename);
        object = new File((File)var2_2, (String)object);
        object = new FileOutputStream((File)object);
        ((FileOutputStream)object).write(arrby);
        ((FileOutputStream)object).close();
    }

    public byte[] load(String filename) throws IOException, NotEnoughDstoresException, FileDoesNotExistException {
        this.f = 0;
        if (filename.contains(" ")) {
            String string = "Filename includes spaces (filename: " + filename + ")";
            ClientLogger.getInstance().error(string);
            throw new IOException(string);
        }
        Object object = "LOAD ".concat(String.valueOf(filename));
        this.e.println((String)object);
        ClientLogger.getInstance().messageSent(this.c.getPort(), (String)object);
        ClientLogger.getInstance().loadStarted(filename);
        object = null;
        try {
            object = this.a(filename);
        }
        catch (a a2) {}
        while (object == null) {
            String string = "RELOAD ".concat(String.valueOf(filename));
            this.e.println(string);
            ClientLogger.getInstance().messageSent(this.c.getPort(), string);
            ClientLogger.getInstance().retryLoad(filename);
            try {
                object = this.a(filename);
            }
            catch (a a3) {}
        }
        return object;
    }

    private byte[] a(String string) throws IOException {
        Object object;
        int n;
        int n2;
        Object object2;
        try {
            object2 = this.d.readLine();
        }
        catch (SocketTimeoutException socketTimeoutException) {
            ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
            throw socketTimeoutException;
        }
        ClientLogger.getInstance().messageReceived(this.c.getPort(), (String)object2);
        if (object2 == null) {
            String string2 = "Connection closed by the Controller";
            ClientLogger.getInstance().error(string2);
            throw new IOException(string2);
        }
        String[] arrstring = ((String)object2).split(" ");
        if (arrstring[0].equals("ERROR_LOAD")) {
            ClientLogger.getInstance().loadFailed(string, this.f);
            throw new IOException("Load operation for file " + string + " failed after having contacted " + this.f + " different Dstores");
        }
        if (arrstring[0].equals("ERROR_FILE_DOES_NOT_EXIST")) {
            ClientLogger.getInstance().fileToLoadDoesNotExist(string);
            throw new FileDoesNotExistException(string);
        }
        if (!arrstring[0].equals("LOAD_FROM")) {
            String string3 = "Unexpected message received (unxpected message: LOAD_FROM): ".concat(String.valueOf(object2));
            ClientLogger.getInstance().error(string3);
            throw new IOException(string3);
        }
        if (arrstring[0].equals("ERROR_NOT_ENOUGH_DSTORES")) {
            ClientLogger.getInstance().error("Not enough Dstores have joined the data store yet");
            throw new NotEnoughDstoresException();
        }
        try {
            n2 = Integer.parseInt(arrstring[1]);
            n = Integer.parseInt(arrstring[2]);
        }
        catch (NumberFormatException numberFormatException) {
            String string4 = "Error parsing LOAD_FROM message to extract Dstore port and filesize. Received message: ".concat(String.valueOf(object2));
            ClientLogger.getInstance().error(string4);
            throw new IOException(string4);
        }
        ClientLogger.getInstance().dstoreWhereToLoadFrom(string, n2, n);
        object2 = null;
        try {
            try {
                ++this.f;
                object2 = new Socket(InetAddress.getLoopbackAddress(), n2);
                ClientLogger.getInstance().connectionEstablished(((Socket)object2).getPort());
                object = new PrintWriter(((Socket)object2).getOutputStream(), true);
                InputStream inputStream = ((Socket)object2).getInputStream();
                String string5 = "LOAD_DATA ".concat(String.valueOf(string));
                ((PrintWriter)object).println(string5);
                ClientLogger.getInstance().messageSent(((Socket)object2).getPort(), string5);
                ClientLogger.getInstance().loadFromDstore(string, n2);
                try {
                    object = inputStream.readNBytes(n);
                }
                catch (SocketTimeoutException socketTimeoutException) {
                    ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
                    throw socketTimeoutException;
                }
                if (((Object)object).length < n) {
                    int n3 = ((Object)object).length;
                    throw new IOException("Expected to read " + n + " bytes, read " + n3 + " bytes instead");
                }
                ClientLogger.getInstance().loadCompleted(string, n2);
            }
            catch (IOException iOException) {
                ClientLogger.getInstance().loadFromDstoreFailed(string, n2);
                throw new a(this, iOException);
            }
        }
        catch (Throwable throwable) {
            if (object2 != null) {
                ((Socket)object2).close();
            }
            throw throwable;
        }
        ((Socket)object2).close();
        return object;
    }

    /*
     * WARNING - void declaration
     */
    public void remove(String filename) throws IOException, NotEnoughDstoresException, FileDoesNotExistException {
        void var1_1;
        Object object = "REMOVE ".concat(String.valueOf(filename));
        this.e.println((String)object);
        ClientLogger.getInstance().messageSent(this.c.getPort(), (String)object);
        ClientLogger.getInstance().removeStarted(filename);
        try {
            object = this.d.readLine();
        }
        catch (SocketTimeoutException socketTimeoutException) {
            ClientLogger.getInstance().timeoutExpiredWhileReading(this.c.getPort());
            ClientLogger.getInstance().removeFailed(filename);
            throw socketTimeoutException;
        }
        ClientLogger.getInstance().messageReceived(this.c.getPort(), (String)object);
        if (object == null) {
            object = "Connection closed by the Controller";
            ClientLogger.getInstance().error((String)object);
            ClientLogger.getInstance().removeFailed(filename);
            throw new IOException((String)object);
        }
        if ((object = object.split(" "))[0].equals("ERROR_FILE_DOES_NOT_EXIST")) {
            ClientLogger.getInstance().fileToRemoveDoesNotExist(filename);
            throw new FileDoesNotExistException(filename);
        }
        if (object[0].equals("REMOVE_COMPLETE")) {
            ClientLogger.getInstance().removeComplete(filename);
            return;
        }
        if (object[0].equals("ERROR_NOT_ENOUGH_DSTORES")) {
            ClientLogger.getInstance().error("Not enough Dstores have joined the data store yet");
            ClientLogger.getInstance().removeFailed(filename);
            throw new NotEnoughDstoresException();
        }
        object = "Unexpected message received. Expected message: REMOVE_COMPLETE";
        ClientLogger.getInstance().error((String)object);
        ClientLogger.getInstance().removeFailed((String)var1_1);
        throw new IOException((String)object);
    }

    private final class a
    extends IOException {
        private static final long serialVersionUID = -5505350949933067170L;

        private a(Client client, IOException iOException) {
            super(iOException);
        }
    }
}

