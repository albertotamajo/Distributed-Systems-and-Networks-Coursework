import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Dstore {

    private final int port;
    private final int timeout;  //timeout is in milliseconds
    private final int controllerPort;
    private String fileFolder;
    private final Map<String,Long> fileSizes;
    private Socket controllerSocket;
    private PrintWriter controllerWriter;
    private BufferedReader controllerReader;


    public Dstore(int port, int controllerPort, int timeout, String fileFolder) {
        this.port = port;
        this.timeout = timeout;
        this.controllerPort = controllerPort;
        this.fileFolder = fileFolder;
        this.fileSizes = Collections.synchronizedMap(new HashMap<>());
        deleteDirContent(new File(fileFolder));

        instantiateDstoreLogger();
    }

    private void instantiateDstoreLogger(){
        try {
            DstoreLogger.init(Loggert.LoggingType.ON_FILE_AND_TERMINAL, port);
        } catch (IOException e) {
            e.printStackTrace(); //TODO how to handle this?
        }
    }

    /********************************** BEGINNING OF REQUEST HANDLING FROM CLIENTS *******************************************/

    public void waitForRequestsFromClients(){

        while(true) {

            try (ServerSocket serverSocket = new ServerSocket(port)) {

                while (true) {
                    Socket client = serverSocket.accept();
                    new Thread(() -> handleRequestFromClients(client)).start();
                }

            } catch (IOException e) {
                //TODO error handling when the server socket cannot be created
            }

        }
    }

    private void handleRequestFromClients(Socket socket){


        try (InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream();
             BufferedReader inReader = new BufferedReader(new InputStreamReader(in)); socket) {

            String sentence = inReader.readLine();
            DstoreLogger.getInstance().messageReceived(socket, sentence);

            switch(commandRecogniser(sentence)){
                case STORE:
                    handleStoreRequest(socket,in,out,sentence);
                    break;
                case REBALANCE_STORE:
                    handleRebalanceStoreRequest(socket,in,out,sentence);
                    break;
                case LOAD_DATA:
                    handleLoadDataRequest(socket,in,out,sentence);
                    break;
                case NoCommand:
                    System.out.println("ERROR => No command recognised from client");
                    break;
            }

        } catch (IOException e) {
            //TODO error handling
        }

    }

    /************************************** END OF REQUEST HANDLING FROM CLIENTS *******************************************/


    /********************************** BEGINNING OF REQUEST HANDLING FROM CONTROLLER **************************************/

    private void waitForRequestsFromController(){

        while(true){
            try {
                String sentence = controllerReader.readLine();
                DstoreLogger.getInstance().messageReceived(controllerSocket, sentence);
                new Thread(() -> {

                    switch (commandRecogniser(sentence)) {

                        case REMOVE:
                            handleRemoveRequest(sentence);
                            break;
                        case LIST:
                            handleListRequest();
                            break;
                        case REBALANCE:
                            handleRebalanceRequest(sentence);
                            break;
                        case NoCommand:
                            System.out.println("ERROR => No command recognised from controller");
                            break;
                    }
                }).start();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /************************************* END OF REQUEST HANDLING FROM CONTROLLER *****************************************/


    /********************************** BEGINNING OF STORE PROCEDURES *******************************************/

    private void handleStoreRequest(Socket socket,InputStream in,OutputStream out,String sentence){

        String[] splitted = sentence.split(" ");

        if(splitted.length != 3){
            System.out.println("ERROR => The STORE command is supposed to have two arguments");
        }else {

            String fileName = splitted[1];
            boolean done = false;

            try (socket; in; out; PrintWriter writer = new PrintWriter(out);
                 FileOutputStream fileOutputStream = createFileOutputStream(fileName))
            {
                long fileSize = Long.parseLong(splitted[2]);

                String message = ClientCommands.ACK.toString();
                writer.println(message); writer.flush();
                DstoreLogger.getInstance().messageSent(socket, message);

                fileOutputStream.write(in.readNBytes((int) fileSize));
                fileSizes.put(fileName,fileSize);

                message = String.format("%s %s", ControllerCommands.STORE_ACK.toString(), fileName);
                synchronized (controllerWriter) {
                    controllerWriter.println(message);
                    controllerWriter.flush();
                }
                DstoreLogger.getInstance().messageSent(controllerSocket, message);
                done = true;

            } catch (IOException e) {
                if(!done){
                    //TODO error handling socket exceptions which are not related to close()
                }

            } catch (NumberFormatException e){
                System.out.println("ERROR => the third argument of the STORE command should be an integer value");
            }
        }
    }

    private FileOutputStream createFileOutputStream(String fileName) throws FileNotFoundException {
        File file = new File(fileFolder + File.separator + fileName);
        return new FileOutputStream(file);
    }

    /*************************************** END OF STORE PROCEDURES *******************************************/


    /*************************************** BEGINNING OF REBALANCE_STORE PROCEDURES ***************************/

    private void handleRebalanceStoreRequest(Socket socket,InputStream in,OutputStream out,String sentence){

        String[] splitted = sentence.split(" ");

        if(splitted.length != 3){
            System.out.println("ERROR => The REBALANCE_STORE command is supposed to have two arguments");
        }else {

            String fileName = splitted[1];
            boolean done = false;

            try (socket; in; out; PrintWriter writer = new PrintWriter(out);
                 FileOutputStream fileOutputStream = createFileOutputStream(fileName))
            {
                long fileSize = Long.parseLong(splitted[2]);

                String message = DstoreCommands.ACK.toString();
                writer.println(message); writer.flush();
                DstoreLogger.getInstance().messageSent(socket, message);

                fileOutputStream.write(in.readNBytes((int) fileSize));
                fileSizes.put(fileName,fileSize);

                done = true;

            } catch (IOException e) {
                if(!done){
                    //TODO error handling socket exceptions which are not related to close()
                }

            } catch (NumberFormatException e){
                System.out.println("ERROR => the third argument of the REBALANCE_STORE command should be an integer value");
            }
        }
    }

    /**************************************** END OF REBALANCE_STORE PROCEDURES *******************************/


    /********************************** BEGINNING OF LOAD_DATA PROCEDURES *******************************************/

    public void handleLoadDataRequest(Socket socket,InputStream in, OutputStream out, String sentence){

        String[] splitted = sentence.split(" ");

        if(splitted.length != 2){
            System.out.println("ERROR => the LOAD_DATA command should take one argument");
        }else {

            String fileName = splitted[1];
            File file = new File(fileFolder + File.separator + fileName);
            boolean done = false;

            try (socket; in; out; PrintWriter writer = new PrintWriter(out);
                 FileInputStream fileInputStream = new FileInputStream(file))
            {

                if (file.exists()) {

                    byte[] bytes = new byte[1024];
                    int len;
                    while ((len = fileInputStream.read(bytes)) != -1) {
                        out.write(bytes, 0, len);
                    }

                }
                done = true;

            } catch (IOException e) {
                if(!done){
                    //TODO error handling for socket exception not related to close()
                }
            }
        }
    }

    /************************************* END OF LOAD_DATA PROCEDURES *******************************************/


    /********************************** BEGINNING OF REMOVE PROCEDURES ******************************************/

    public void handleRemoveRequest(String sentence){

        String[] splitted = sentence.split(" ");

        if(splitted.length != 2){
            System.out.println("ERROR => the REMOVE command should take one argument");
        }else {
            String fileName = splitted[1];

            File file = new File(fileFolder + File.separator + fileName);
            String message;

            if (file.exists()) {
                file.delete();
                message = String.format("%s %s", ControllerCommands.REMOVE_ACK.toString(), fileName);
                synchronized (controllerWriter) {
                    controllerWriter.println(message);
                    controllerWriter.flush();
                }
                DstoreLogger.getInstance().messageSent(controllerSocket, message);
            } else {
                message = String.format("%s %s", ControllerCommands.ERROR_FILE_DOES_NOT_EXIST, fileName);
                synchronized (controllerWriter) {
                    controllerWriter.println(message);
                    controllerWriter.flush();
                }
                DstoreLogger.getInstance().messageSent(controllerSocket, message);
            }
        }
    }

    /********************************** END OF REMOVE PROCEDURES *******************************************/


    /********************************** BEGINNING OF REBALANCE PROCEDURES *******************************************/

    private void handleRebalanceRequest(String sentence){

        List<String> splitted = Arrays.stream(sentence.split(" ")).collect(Collectors.toList());

        if (splitted.size() < 2) {
            System.out.println("Error: the REBALANCE command should take more than one argument");
        } else {

            try {
                var filesToSendRemove = parseRebalanceSentence(splitted);
                if (sendStoreRequests(filesToSendRemove.fst)) { //TODO should the files be removed anyway or just when all send store requests are successful?
                    removeFiles(filesToSendRemove.snd);

                    String message = ControllerCommands.REBALANCE_COMPLETE.toString();
                    synchronized (controllerWriter) {
                        controllerWriter.println(message);
                        controllerWriter.flush();
                    }
                    DstoreLogger.getInstance().messageSent(controllerSocket, message);
                }

            } catch (Exception e) {
                System.out.println("Error: the REBALANCE message was malformed");
            }
        }
    }

    private Pair<List<Pair<String,List<Integer>>>,List<String>> parseRebalanceSentence(List<String> splitted){
        splitted.remove(0);
        var filesToSend = parseFilesToSend(splitted);
        var filesToRemove = parseFilesToRemove(splitted);
        return new Pair<>(filesToSend,filesToRemove);
    }

    private List<Pair<String,List<Integer>>> parseFilesToSend(List<String> splitted){
        List<Pair<String,List<Integer>>> list = new ArrayList<>();
        int numberFiles = Integer.parseInt(splitted.get(0));
        splitted.remove(0);
        for(int i = 1; i <= numberFiles; i++){
            list.add(parseFileToSend(splitted));
        }
        return list;
    }

    private Pair<String,List<Integer>> parseFileToSend(List<String> splitted){
        String file = splitted.get(0);
        splitted.remove(0);
        int numberPorts = Integer.parseInt(splitted.get(0));
        splitted.remove(0);
        List<Integer> listPortsToSend = splitted.stream().limit(numberPorts).map(Integer::parseInt).collect(Collectors.toList());
        for(int i = 1; i <= numberPorts; i++){
            splitted.remove(0);
        }
        return new Pair<>(file,listPortsToSend);
    }

    private List<String> parseFilesToRemove(List<String> splitted){

        int numberFilesToRemove = Integer.parseInt(splitted.get(0));
        splitted.remove(0);
        return splitted.stream().limit(numberFilesToRemove).collect(Collectors.toList());
    }

    private void removeFiles(List<String> fileNames){
        for(String fileName : fileNames)
            removeFile(fileName);
    }

    private void removeFile(String fileName){
        File file = new File(fileFolder + File.separator + fileName);
        if(file.exists()){
            try{
                file.delete();
            } catch (Exception e) {
                //TODO error handling file cannot be deleted
            }
        }else{
            //TODO error handling file does not exist
        }
    }

    /************************************** END OF REBALANCE PROCEDURES ********************************************/

    /************************************** BEGINNING OF SEND STORE REQUEST ********************************************/

    private boolean sendStoreRequests(List<Pair<String,List<Integer>>> filesToSend){

        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (var pair : filesToSend) {
            for(int port : pair.snd){
                tasks.add(() -> sendStoreRequest(pair.fst,port));
            }
        }
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(10);
            int numberFailures = (int) executorService.invokeAll(tasks).stream()
                    .map(r -> {
                        try {
                            return r.get();
                        } catch (InterruptedException | ExecutionException e) {
                            return false;
                        }
                    })
                    .filter(r -> !r)
                    .count();

            return numberFailures == 0;

        } catch (InterruptedException e) {
            //TODO error handling even though invoke all will never throw such exception
            return false;
        }
    }

    private boolean sendStoreRequest(String fileName, int dstorePort){
        boolean done = false;
        boolean result = false;

        try(Socket socket = new Socket(InetAddress.getLocalHost(),dstorePort);
            OutputStream out = socket.getOutputStream();
            PrintWriter writer = new PrintWriter(out);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            FileInputStream fileInputStream = createFileInputStream(fileName)
            )
        {
            String message = String.format("%s %s %s",DstoreCommands.REBALANCE_STORE.toString(),fileName,fileSizes.get(fileName));
            writer.println(message); writer.flush();
            DstoreLogger.getInstance().messageSent(socket, message);

            String answer = reader.readLine();
            DstoreLogger.getInstance().messageReceived(socket, answer);

            if(commandRecogniser(answer)==DstoreCommands.ACK){
                byte[] bytes = new byte[1024];
                int bytesLen;
                while ((bytesLen = fileInputStream.read(bytes)) != -1) {
                    out.write(bytes, 0, bytesLen);
                }
                done = true;
                result=true;
                return result;
            }else{
                System.out.println("ERROR => After REBALANCE_STORE command issued, ACK command is expected back");
                done = true;
                return result;
            }
        } catch (IOException e) {
            if(!done) {
                //TODO error handling socket exceptions
            }
            return result;
        }
    }

    private FileInputStream createFileInputStream(String fileName) throws FileNotFoundException{
        return new FileInputStream(fileFolder + File.separator + fileName);
    }

    /***************************************** END OF SEND STORE REQUEST ***********************************************/


    /***************************************** BEGINNING OF JOIN REQUEST ***********************************************/

    public boolean joinController(){

        try{
            Socket socket = new Socket(InetAddress.getLocalHost(),controllerPort);
            PrintWriter writer = new PrintWriter(socket.getOutputStream());
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String message = String.format("%s %s",ControllerCommands.JOIN,port);
            writer.println(message); writer.flush();
            DstoreLogger.getInstance().messageSent(socket, message);


            controllerSocket = socket;
            controllerWriter = writer;
            controllerReader = reader;


            return true;

        } catch (IOException e) {
            return false;
        }
    }

    /******************************************** END OF JOIN REQUEST *********************************************+++**/

    /****************************************** BEGINNING OF LIST REQUEST **************************************+++**/

    private void handleListRequest(){

        File file = new File(fileFolder);
        String[] fileList = file.list();
        String message = ControllerCommands.LIST.toString();
        if (fileList != null) {
            if (fileList.length == 0) {
                message = message + " ";
                synchronized (controllerWriter) {
                    controllerWriter.println(message);
                    controllerWriter.flush();
                }
                DstoreLogger.getInstance().messageSent(controllerSocket, message);

            } else {
                message = message + " " + String.join(" ", fileList);
                synchronized (controllerWriter) {
                    controllerWriter.println(message);
                    controllerWriter.flush();
                }
                DstoreLogger.getInstance().messageSent(controllerSocket, message);
            }
        } else {
            //TODO error handling fileFolder does not exist
        }

    }


    /******************************************** END OF LIST REQUEST *********************************************+++**/



    /***************************************** BEGINNING OF COMMAND RECOGNISER ***********************************************/

    private DstoreCommands commandRecogniser(byte[] array, int length){

        String wholeSentence = new String(array,0,length);
        int firstSpaceIndex = wholeSentence.indexOf(" ");
        String command = wholeSentence.substring(0,firstSpaceIndex);

        if  (command.equals(DstoreCommands.STORE.toString()))
            return DstoreCommands.STORE;
        else if (command.equals(DstoreCommands.LOAD_DATA.toString()))
            return DstoreCommands.LOAD_DATA;
        else if (command.equals(DstoreCommands.REMOVE.toString()))
            return DstoreCommands.REMOVE;
        else if (command.equals(DstoreCommands.LIST.toString()))
            return DstoreCommands.LIST;
        else if (command.equals(DstoreCommands.REBALANCE.toString()))
            return DstoreCommands.REBALANCE;
        else if (command.equals(DstoreCommands.ACK.toString()))
            return DstoreCommands.ACK;
        else
            return DstoreCommands.NoCommand;

    }

    private DstoreCommands commandRecogniser(String sentence){

        String[] splitted = sentence.split(" ");
        String command =splitted[0];

        if  (command.equals(DstoreCommands.STORE.toString()))
            return DstoreCommands.STORE;
        else if (command.equals(DstoreCommands.LOAD_DATA.toString()))
            return DstoreCommands.LOAD_DATA;
        else if (command.equals(DstoreCommands.REMOVE.toString()))
            return DstoreCommands.REMOVE;
        else if (command.equals(DstoreCommands.LIST.toString()))
            return DstoreCommands.LIST;
        else if (command.equals(DstoreCommands.REBALANCE.toString()))
            return DstoreCommands.REBALANCE;
        else if (command.equals(DstoreCommands.ACK.toString()))
            return DstoreCommands.ACK;
        else if (command.equals(DstoreCommands.REBALANCE_STORE.toString()))
            return DstoreCommands.REBALANCE_STORE;
        else
            return DstoreCommands.NoCommand;

    }

    /*********************************************GETTERS AND SETTERS****************************************************/

    public int getPort() {
        return port;
    }
    public void setFileFolder(String fileFolder) {
        this.fileFolder = fileFolder;
    }

    public String getFileFolder() {
        return fileFolder;
    }

    public Map<String, Long> getFileSizes() {
        return fileSizes;
    }

    private void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if(files != null) {
            for (File file : files) {
                deleteDir(file);
            }
        }
        dir.delete();
    }

    private void deleteDirContent(File dir){
        File[] files = dir.listFiles();
        for(var file : files){
            deleteDir(file);
        }
    }

    /*********************************************GETTERS AND SETTERS****************************************************/

    /*******************************************+STARTING DSTORE*********************************************************/
    public void start(){
        while(true){
            if (joinController())
                break;
        };
        new Thread(this::waitForRequestsFromClients).start();
        new Thread(this::waitForRequestsFromController).start();

    }


    /******************************************** END OF COMMAND RECOGNISER ***********************************************/



    public static void main(String[] args) {
        Dstore dstore = new Dstore(Integer.valueOf(args[0]),Integer.valueOf(args[1]),Integer.valueOf(args[2]),args[3]);
        dstore.start();
    }

}
