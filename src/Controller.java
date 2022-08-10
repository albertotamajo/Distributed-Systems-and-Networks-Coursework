import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Controller {

    private final int port;
    private final int replicationFactor;
    private final int timeout;  //timeout is in milliseconds
    private final int rebalancePeriod;  //rebalancePeriod is in milliseconds
    private final List<Integer> dstorePorts;
    private final Map<Integer,Socket> dstorePortsSocket;
    private final Map<String,List<Integer>> fileAllocation;
    private final Pair<List<Integer>,List<Pair<Integer,List<String>>>> filesIndStore;
    private final Map<String,Long> fileSizes;
    private final Map<String,Pair<AtomicInteger,Pair<Socket,Pair<Long,List<Integer>>>>> acknowledges;
    private final Pair<Pair<AtomicInteger,AtomicInteger>,HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>>> rebalancesCompleted;
    private final Map<Socket,List<Integer>> suspendedReloads;
    private final AtomicBoolean alreadyRebalancing;
    private final AtomicBoolean storingOperation;
    private final AtomicBoolean removingOperation;
    private boolean canRebalance;
    private final List<Socket> socketsClosedForTimeout;


    public Controller(int port, int replicationFactor, int timeout, int rebalancePeriod) {
        this.port = port;
        this.canRebalance = true;
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        this.dstorePorts = Collections.synchronizedList(new ArrayList<>());
        this.dstorePortsSocket = Collections.synchronizedMap(new HashMap<>());
        this.fileAllocation = Collections.synchronizedMap(new HashMap<>());
        this.filesIndStore = new Pair<>(Collections.synchronizedList(new ArrayList<>()),Collections.synchronizedList(new ArrayList<>()));
        this.fileSizes = Collections.synchronizedMap(new HashMap<>());
        this.acknowledges = Collections.synchronizedMap(new HashMap<>());
        this.alreadyRebalancing = new AtomicBoolean(false);
        this.rebalancesCompleted = new Pair<>(new Pair<>(new AtomicInteger(0),new AtomicInteger(0)),null);
        this.suspendedReloads = Collections.synchronizedMap(new HashMap<>());
        this.storingOperation = new AtomicBoolean(false);
        this.removingOperation = new AtomicBoolean(false);
        this.socketsClosedForTimeout = Collections.synchronizedList(new ArrayList<>());

        instantiateControllerLogger();
    }

    private void instantiateControllerLogger(){
        try {
            ControllerLogger.init(Loggert.LoggingType.ON_FILE_AND_TERMINAL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*****************************************BEGINNING OF HANDLE REQUESTS FROM CLIENTS*************************************/

    private void waitForRequestsFromClients(){

        try (ServerSocket serverSocket = new ServerSocket(port)) {

            while(true){
                Socket client = serverSocket.accept();
                new Thread(()-> handleRequest(client)).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
            //TODO error handling when the server socket cannot be created
        }
    }

    private void handleRequest(Socket socket){

        try{

            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            BufferedReader inReader = new BufferedReader(new InputStreamReader(in));
            PrintWriter writer = new PrintWriter(out);
            boolean notDstore = true;

            while(notDstore) {

                String sentence;

                try {
                    sentence = inReader.readLine();
                    //the socket's other end has closed the connection
                    if (sentence == null) {
                        throw new IOException();
                    }
                }catch(IOException e){
                    cleanUpAfterClientDisconnects(socket);
                    break;
                }

                while (alreadyRebalancing.get()) {
                    //Loops here as long as the rebalance operation is performing
                }

                switch (commandRecogniser(sentence)) {
                    case STORE:
                        ControllerLogger.getInstance().messageReceived(socket, sentence);
                        storingOperation.set(true);
                        new Thread(()-> handleStoreRequest(socket,writer,sentence)).start();
                        break;
                    case LOAD:
                        ControllerLogger.getInstance().messageReceived(socket, sentence);
                        new Thread(()->handleLoadRequest(writer, socket, sentence)).start();
                        break;
                    case JOIN:
                        ControllerLogger.getInstance().messageReceived(socket, sentence);
                        new Thread(()->handleJoinRequest(socket, sentence)).start();
                        notDstore = false;
                        break;
                    case LIST:
                        ControllerLogger.getInstance().messageReceived(socket, sentence);
                        new Thread(()->handleListRequest(writer, socket)).start();
                        break;
                    case REMOVE:
                        ControllerLogger.getInstance().messageReceived(socket, sentence);
                        removingOperation.set(true);
                        new Thread(()->handleRemoveRequest(writer, socket, sentence)).start();
                        break;
                    case RELOAD:
                        ControllerLogger.getInstance().messageReceived(socket, sentence);
                        new Thread(()->handleReloadRequest(writer, socket, sentence)).start();
                        break;
                    case NoCommand:
                        System.out.println("ERROR => no command recognised from client");
                }
            }
        } catch (IOException e) {
            //TODO error handling
        }
    }

    private void cleanUpAfterClientDisconnects(Socket socket){
        suspendedReloads.remove(socket);
    }

    /****************************************END OF OF HANDLE REQUESTS FROM CLIENTS**********************************/


    /**************************************BEGINNING OF HANDLE REQUESTS FROM DSTORE*******************************/

    private void waitForRequestsDstore(Socket socket){

        try{

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            while (true) {
                String sentence;

                try{
                    sentence = reader.readLine();
                    //the other end has closed the connection
                    if(sentence == null){
                        throw new IOException();
                    }

                }catch (IOException e){
                    if(socketsClosedForTimeout.contains(socket)){
                        System.out.println("I have been closed");
                        cleanUpAfterDstoreNotListingWithinTimeout(socket);
                        socketsClosedForTimeout.remove(socket);
                    }else{
                        cleanUpAfterDstoreDisconnects(socket);
                    }
                    break;
                }

                ControllerLogger.getInstance().messageReceived(socket, sentence);

                new Thread(() -> {
                    switch (commandRecogniser(sentence)) {
                        case STORE_ACK:
                            handleStoreAck(sentence);
                            break;
                        case REMOVE_ACK:
                            handleRemoveAck(sentence);
                            break;
                        case ERROR_FILE_DOES_NOT_EXIST:
                            handleErrorDoesNotExist(sentence);
                            break;
                        case LIST:
                            handleList(socket,sentence);
                            break;
                        case REBALANCE_COMPLETE:
                            handleRebalanceComplete();
                            break;
                        case NoCommand:
                            System.out.println("ERROR => no command recognised from dstore");
                            break;
                    }
                }).start();
            }
        }catch (IOException e){
            //TODO error handling
        }
    }

    private void cleanUpAfterDstoreNotListingWithinTimeout(Socket socket){
        int port = getDstorePort(socket);
        dstorePorts.remove((Integer) port);
        dstorePortsSocket.remove(port);
        cleanUpSuspendedReloads(port);
        cleanUpAcknowledges(port);
    }

    private void cleanUpAfterDstoreDisconnects(Socket socket){
        int port = getDstorePort(socket);
        dstorePorts.remove((Integer) port);
        dstorePortsSocket.remove(port);
        cleanUpFiles(port);
        cleanUpSuspendedReloads(port);
        cleanUpAcknowledges(port);
        cleanUpFilesInDstores(port);
        restartRebalanceOperation();
    }

    private void cleanUpFiles(int port){
        cleanUpFileAllocation(port);
        cleanUpFileSizes();
    }

    private void cleanUpFileAllocation(int port) {
        synchronized (fileAllocation) {
            Iterator<String> iter = fileAllocation.keySet().iterator();
            while(iter.hasNext()){
                String file = iter.next();
                fileAllocation.get(file).remove((Integer) port);
                if(fileAllocation.get(file).size() == 0){
                    iter.remove();
                }
            }
        }
    }

    private void cleanUpFileSizes(){
        synchronized (this.fileSizes) {
            Iterator<String> iter = fileSizes.keySet().iterator();
            while(iter.hasNext()){
                String file = iter.next();
                if (!this.fileAllocation.containsKey(file))
                    iter.remove();
            }
        }
    }

    private void cleanUpSuspendedReloads(int port){
        synchronized (suspendedReloads){
            for(var socket : suspendedReloads.keySet()){
                suspendedReloads.get(socket).remove((Integer) port);
            }
        }
    }

    private void cleanUpAcknowledges(int port){

        synchronized (acknowledges){
            for(var file : acknowledges.keySet()){
                acknowledges.get(file).snd.snd.snd.remove((Integer)port);
            }
        }

    }

    private void cleanUpFilesInDstores(int port){
        synchronized (filesIndStore) {
            filesIndStore.fst.remove((Integer) port);
            filesIndStore.snd = filesIndStore.snd.stream().filter(p -> p.fst != port).collect(Collectors.toList());
        }
    }

    private int getDstorePort(Socket socket){
        synchronized (dstorePortsSocket) {
            for (int port : dstorePortsSocket.keySet()){
                if(dstorePortsSocket.get(port)==socket){
                    return port;
                }
            }
        }
        //Not possible to return 0
        return 0;
    }


    /******************************************END OF HANDLE REQUESTS FROM DSTORE********************************/


    /********************************** BEGINNING OF STORE PROCEDURES *******************************************/

    private void handleStoreRequest(Socket socket, PrintWriter writer, String sentence){

        String[] splitted = sentence.split(" ");

        if (splitted.length != 3) {
            storingOperation.set(false);
            System.out.println("ERROR => The STORE command should take two arguments");
        } else {

            String fileName = splitted[1];

            try{
                String message;

                if(dstorePorts.size() < replicationFactor){

                    storingOperation.set(false);
                    message = ClientCommands.ERROR_NOT_ENOUGH_DSTORES.toString();
                    writer.println(message); writer.flush();
                    ControllerLogger.getInstance().messageSent(socket, message);
                    return;
                }

                long fileSize = Long.parseLong(splitted[2]);


                //TODO should a file being removed result as already existing?
                if (fileAllocation.containsKey(fileName) || acknowledges.containsKey(fileName)) {
                    storingOperation.set(false);
                    message = ClientCommands.ERROR_FILE_ALREADY_EXISTS.toString();
                    writer.println(message);
                    writer.flush();
                    ControllerLogger.getInstance().messageSent(socket, message);

                } else {

                    List<Integer> dports = rEmptierDports();

                    message = String.format("%s %s", ClientCommands.STORE_TO.toString(), listDportsToString(dports));
                    writer.println(message);
                    writer.flush();
                    ControllerLogger.getInstance().messageSent(socket, message);
                    acknowledges.put(fileName, new Pair<>(new AtomicInteger(0), new Pair<>(socket, new Pair<>(fileSize, dports))));

                    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
                    scheduler.schedule(()->{
                        if (acknowledges.containsKey(fileName)) {
                            storingOperation.set(false);
                            System.out.println(String.format("ERROR => %s store acknowledges have not been received prior to the timeout",replicationFactor));
                            acknowledges.remove(fileName);
                        }
                    },timeout,TimeUnit.MILLISECONDS);

                }
            } catch (NumberFormatException e) {
                storingOperation.set(false);
                System.out.println("ERROR => The third argument of the STORE command should be an integer value");
            }
        }
    }

    private String listDportsToString (List<Integer> list){
        return list.stream()
                .map(Objects::toString)
                .collect(Collectors.joining(" "));
    }

    private List<Integer> rEmptierDports(){

        return pairsNumberOfFilesDstorePorts().stream()
                .sorted(Comparator.comparingInt(x -> x.snd))
                .map(x -> x.fst)
                .limit(replicationFactor)
                .collect(Collectors.toList());
    }

    private List<Pair<Integer,Integer>> pairsNumberOfFilesDstorePorts(){
        List<Pair<Integer,Integer>> pairs = new ArrayList<>();
        synchronized (dstorePorts) {
            for (int port : dstorePorts) {
                pairs.add(new Pair<>(port,computeNumberFilesAllocatedDstore(port)));
            }
        }
        return pairs;
    }

    private int computeNumberFilesAllocatedDstore(int dstorePort){
        int counter = 0;
        synchronized (fileAllocation) {
            for (String file : fileAllocation.keySet()) {
                if (fileAllocation.get(file).contains(dstorePort))
                    counter++;
            }
        }
        return counter;
    }

    /***************************************** END OF STORE PROCEDURES *******************************************/


    /*************************************** BEGINNING OF ACK PROCEDURES *******************************************/

    private void handleStoreAck(String sentence){
        String[] splitted = sentence.split(" ");

        if(splitted.length != 2){
            System.out.println("ERROR => the STORE_ACK command should take one argument");
        }else{
            String fileName = splitted[1];

            if(acknowledges.containsKey(fileName)){

                if(acknowledges.get(fileName).fst.incrementAndGet() == replicationFactor) {
                    storingOperation.set(false);
                    sendStoreComplete(acknowledges.get(fileName).snd.fst);
                    fileAllocation.put(fileName, acknowledges.get(fileName).snd.snd.snd);
                    fileSizes.put(fileName, acknowledges.get(fileName).snd.snd.fst);
                    acknowledges.remove(fileName);
                }

            }else{
                System.out.println("ERROR => an acknowledge has arrived late");
            }
        }
    }

    private void sendStoreComplete(Socket socket){

        try {

            PrintWriter writer = new PrintWriter(socket.getOutputStream());
            String message = ClientCommands.STORE_COMPLETE.toString();
            writer.println(message);
            writer.flush();
            ControllerLogger.getInstance().messageSent(socket, message);

        }catch(IOException e){
            //Nothing to do
        }
    }

    /****************************************** END OF ACK PROCEDURES *******************************************/


    /*************************************** BEGINNING OF LOAD PROCEDURES *******************************************/

    public void handleLoadRequest(PrintWriter writer, Socket socket, String sentence){

        String[] splitted = sentence.split(" ");

        if (splitted.length != 2){
            System.out.println("ERROR => the LOAD command should take one argument");
        }else{

            String fileName = splitted[1];
            String message;

            if(dstorePorts.size() < replicationFactor){

                message = ClientCommands.ERROR_NOT_ENOUGH_DSTORES.toString();
                writer.println(message); writer.flush();
                ControllerLogger.getInstance().messageSent(socket, message);
                return;
            }

            if (!fileAllocation.containsKey(fileName)) {
                message = ClientCommands.ERROR_FILE_DOES_NOT_EXIST.toString();
                writer.println(message);
                writer.flush();
                ControllerLogger.getInstance().messageSent(socket, message);

            } else {
                addSuspendedReloads(socket,fileAllocation.get(fileName).stream().skip(1).collect(Collectors.toList()));
                int port = fileAllocation.get(fileName).get(0);
                long fileSize = fileSizes.get(fileName);
                message = String.format("%s %s %s", ClientCommands.LOAD_FROM.toString(), port, fileSize);
                writer.println(message);
                writer.flush();
                ControllerLogger.getInstance().messageSent(socket, message);
            }

        }
    }

    private void addSuspendedReloads(Socket socket, List<Integer> dstorePorts){
        this.suspendedReloads.put(socket,dstorePorts);
    }

    /*************************************** END OF LOAD PROCEDURES *******************************************/

    /************************************ BEGINNING OF RELOAD PROCEDURES *******************************************/

    private void handleReloadRequest(PrintWriter writer, Socket socket, String sentence){
        String[] splitted = sentence.split(" ");
        if(splitted.length != 2){
            System.out.println("ERROR => the RELOAD command should have one argument");
        }else{
            String filename = splitted[1];

            if(! suspendedReloads.containsKey(socket)){
                System.out.println("ERROR => it is not possible to use the RELOAD command prior to LOAD");
            }else{
                String message;
                if(suspendedReloads.get(socket).size()==0){
                    suspendedReloads.remove(socket);
                    message = ClientCommands.ERROR_LOAD.toString();
                    writer.println(message); writer.flush();
                    ControllerLogger.getInstance().messageSent(socket, message);
                }else{
                    int port = suspendedReloads.get(socket).get(0);
                    long fileSize = fileSizes.get(filename);
                    suspendedReloads.get(socket).remove(0);
                    message = String.format("%s %s %s", ClientCommands.LOAD_FROM.toString(), port, fileSize);
                    writer.println(message); writer.flush();
                    ControllerLogger.getInstance().messageSent(socket, message);
                }
            }
        }
    }

    /*************************************** END OF RELOAD PROCEDURES *******************************************/



    /*************************************** BEGINNING OF JOIN PROCEDURES *******************************************/

    private void handleJoinRequest(Socket socket, String sentence){
        String[] splitted = sentence.split(" ");

        if (splitted.length != 2){
            System.out.println("ERROR => the JOIN command should take one argument");
        }else{

            try {
                int port = Integer.parseInt(splitted[1]);

                if(! dstorePorts.contains(port)) {
                    ControllerLogger.getInstance().dstoreJoined(socket, port);
                    dstorePorts.add(port);
                    dstorePortsSocket.put(port,socket);
                    new Thread(()->waitForRequestsDstore(socket)).start();

                    if(dstorePorts.size() >= replicationFactor){
                        System.out.println("Rebalance after joining");
                        new Thread(()->{
                            while(!rebalanceOperation()){
                            }
                        }).start();
                    }

                }

            } catch (NumberFormatException e) {
                System.out.println("Error: the second argument of the JOIN command should be an integer number");
            }
        }
    }

    /*************************************** END OF JOIN PROCEDURES *******************************************/


    /*************************************** BEGINNING OF LIST PROCEDURES *******************************************/

    private void handleListRequest(PrintWriter writer, Socket socket){


        if (dstorePorts.size() < replicationFactor) {

            String message = ClientCommands.ERROR_NOT_ENOUGH_DSTORES.toString();
            writer.println(message);
            writer.flush();
            ControllerLogger.getInstance().messageSent(socket, message);
            return;
        }

        String fileList;
        synchronized (fileAllocation) {
            if (fileAllocation.size() != 0) {
                Set<String> files = new HashSet<>(fileAllocation.keySet());
                files.removeAll(acknowledges.keySet());
                fileList = String.join(" ", files);
            } else {
                fileList = "";
            }
        }

        writer.println(String.format("%s %s",ClientCommands.LIST.toString(),fileList));
        writer.flush();
        ControllerLogger.getInstance().messageSent(socket, fileList);
    }

    /****************************************** END OF LIST PROCEDURES *********************************************/


    /*************************************** BEGINNING OF REMOVE PROCEDURES *******************************************/

    private void handleRemoveRequest(PrintWriter writer, Socket socket, String sentence){

        String[] splitted = sentence.split(" ");
        if(splitted.length != 2){
            removingOperation.set(false);
            System.out.println("Error => the REMOVE command should take one argument");
        }else{

            String fileName = splitted[1];
            String message;

            if (dstorePorts.size() < replicationFactor) {

                removingOperation.set(false);
                message = ClientCommands.ERROR_NOT_ENOUGH_DSTORES.toString();
                writer.println(message);
                writer.flush();
                ControllerLogger.getInstance().messageSent(socket, message);
                return;
            }

            if (fileAllocation.containsKey(fileName) && (! acknowledges.containsKey(fileName))) {
                //if the file is present but it is not being removed

                List<Integer> dstores = new ArrayList<>(fileAllocation.get(fileName));
                acknowledges.put(fileName, new Pair<>(new AtomicInteger(0), new Pair<>(socket, new Pair<>(0L, dstores))));
                sendRemoveToDstores(dstores, fileName);

                ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
                scheduler.schedule(()->{
                    if (acknowledges.containsKey(fileName)) {
                        removingOperation.set(false);
                        System.out.println(String.format("ERROR => %s remove acknowledges have not been received prior to the timeout",dstores.size()));
                        acknowledges.remove(fileName);
                    }
                }, timeout,TimeUnit.MILLISECONDS);

            } else {
                removingOperation.set(false);
                message = ClientCommands.ERROR_FILE_DOES_NOT_EXIST.toString();
                writer.println(message);
                writer.flush();
                ControllerLogger.getInstance().messageSent(socket, message);
            }
        }
    }

    private void sendRemoveToDstores(List<Integer> dstorePorts, String fileName){
        for(int dstore : dstorePorts){
            new Thread(()->sendRemoveToDstore(dstore,fileName)).start();
        }
    }

    private void  sendRemoveToDstore(int dstorePort, String fileName){

        try {
            Socket socket = dstorePortsSocket.get(dstorePort);
            PrintWriter writer = new PrintWriter(socket.getOutputStream());
            String message = String.format("%s %s",DstoreCommands.REMOVE.toString(),fileName);
            writer.println(message); writer.flush();
            ControllerLogger.getInstance().messageSent(socket,message);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /****************************************** END OF REMOVE PROCEDURES *******************************************/


    /****************************************** BEGINNING OF REMOVE ACK REQUEST ************************************/

    private void handleRemoveAck(String sentence){
        String[] splitted = sentence.split(" ");

        if(splitted.length != 2){
            System.out.println("ERROR => the REMOVE_ACK command should take one argument");
        }else{
            String fileName = splitted[1];

            if(acknowledges.containsKey(fileName)){

                if(acknowledges.get(fileName).fst.incrementAndGet() >= acknowledges.get(fileName).snd.snd.snd.size()) {
                    removingOperation.set(false);
                    sendRemoveComplete(acknowledges.get(fileName).snd.fst);
                    fileAllocation.remove(fileName);
                    fileSizes.remove(fileName);
                    acknowledges.remove(fileName);
                }

            }else{
                System.out.println("ERROR => a remove acknowledge has arrived late");
            }
        }
    }

    private void sendRemoveComplete(Socket socket){

        try {
            PrintWriter writer = new PrintWriter(socket.getOutputStream());
            String message = ClientCommands.REMOVE_COMPLETE.toString();
            writer.println(message);
            writer.flush();
            ControllerLogger.getInstance().messageSent(socket, message);
        } catch(IOException e){
            //Nothing to do here
        }

    }

    /******************************************* END OF REMOVE ACK REQUEST ***************************************/


    /****************************************** BEGINNING OF ERROR DOES NOT EXIST*********************************/

    private void handleErrorDoesNotExist(String sentence){

        String[] splitted = sentence.split(" ");

        if(splitted.length != 2){
            System.out.println("ERROR => the ERROR_FILE_DOES_NOT_EXIST command should take one argument");
        }else{
            String fileName = splitted[1];

            if(acknowledges.containsKey(fileName)){

                if(acknowledges.get(fileName).fst.incrementAndGet() >= acknowledges.get(fileName).snd.snd.snd.size()) {
                    removingOperation.set(false);
                    sendRemoveComplete(acknowledges.get(fileName).snd.fst);
                    fileAllocation.remove(fileName);
                    fileSizes.remove(fileName);
                    acknowledges.remove(fileName);
                }

            }else{
                System.out.println("ERROR => an error file does not exist ackowledge has arrived late");
            }
        }
    }

    /********************************************* END OF ERROR DOES NOT EXIST **********************************/



    /************************************** BEGINNING OF LIST PROCEDURE ******************************************/

    private void handleList(Socket socket, String sentence){

        String[] splitted = sentence.split(" ");
        int dstore = 0;

        synchronized (dstorePortsSocket){
            for(int dsto : dstorePortsSocket.keySet()){
                if(dstorePortsSocket.get(dsto) == socket){
                    dstore = dsto;
                    break;
                }
            }
        }

        if(splitted.length == 1){
            synchronized (filesIndStore){
                filesIndStore.snd.add(new Pair<>(dstore,new ArrayList<>(){{add("");}}));
            }
        }else{
            synchronized (filesIndStore) {
                filesIndStore.snd.add(new Pair<>(dstore, Arrays.stream(splitted).skip(1).collect(Collectors.toList())));
            }
        }

        restartRebalanceOperation();
    }

    private void restartRebalanceOperation(){
        synchronized (filesIndStore) { //synch block needed as two threds could arrive at the same time and start two rebalances
            if ((filesIndStore.fst.size() == filesIndStore.snd.size()) && (filesIndStore.fst.size() != 0)) {
                //Restart rebalance operation
                var filesInDstoreSnd = shallowCopy(filesIndStore.snd);
                clearFilesIndStore();
                new Thread(() -> rebalanceOperationAfterListOperations(filesInDstoreSnd)).start();
            }
        }
    }

    private void clearFilesIndStore(){
        filesIndStore.fst.clear();
        filesIndStore.snd.clear();
    }

    /****************************************** END OF LIST PROCEDURE *******************************************/


    /*************************************** BEGINNING OF REBALANCE PROCEDURES *******************************************/


    private void scheduleRebalanceOperation(){
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if(dstorePorts.size() >= replicationFactor) {
                    rebalanceOperation();
                }
            }
        };

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(task,0,rebalancePeriod);
    }

    private boolean rebalanceOperation(){

        if( canRebalance && fileAllocation.size() != 0 && alreadyRebalancing.compareAndSet(false,true)) {

            while(storingOperation.get() || removingOperation.get()){
                //Loops here as long as there is no storing or removing operation being performed
            }

            performListOperations();
            return true;

        }
        return (fileAllocation.size() == 0) || false;
    }

    private void performListOperations(){

        synchronized (dstorePorts) {
            filesIndStore.fst.addAll(dstorePorts);
            filesIndStore.snd.clear();
            for (int port : dstorePorts) {
                new Thread(() -> listOperation(port)).start();
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(()->{
            synchronized (filesIndStore){
                if(filesIndStore.fst.size() != 0){
                    //Restart rebalance operation
                    var filesInDstoreSnd = shallowCopy(filesIndStore.snd);
                    closeDstoresFailed();
                    clearFilesIndStore();
                    new Thread(() -> rebalanceOperationAfterListOperations(filesInDstoreSnd)).start();
                }
            }
        }, timeout,TimeUnit.MILLISECONDS);

    }

    private void closeDstoresFailed(){
        var dstoreFailed = getFailedDstores();
        for(int port : dstoreFailed){
            try {
                socketsClosedForTimeout.add(dstorePortsSocket.get(port));
                dstorePortsSocket.get(port).close();
            }catch (IOException e){

            }
        }
    }

    private List<Integer> getFailedDstores(){
        var dstoresNotFailed = filesIndStore.snd.stream()
                .map(p -> p.fst)
                .collect(Collectors.toList());

        var dstoreFailed = filesIndStore.fst.stream()
                .filter(d -> !dstoresNotFailed.contains(d))
                .collect(Collectors.toList());

        return dstoreFailed;
    }

    private void listOperation(int dstorePort) {

        try {
            Socket socket = dstorePortsSocket.get(dstorePort);
            PrintWriter writer = new PrintWriter(socket.getOutputStream());
            String message = DstoreCommands.LIST.toString();
            writer.println(message);
            writer.flush();
            ControllerLogger.getInstance().messageSent(socket, message);

        }catch(IOException e){
            //TODO error handling
        }catch(Exception e){

        }
    }

    private void rebalanceOperationAfterListOperations(List<Pair<Integer, List<String>>> filesInDstores){

        List<Pair<String, List<Integer>>> fileAllocation = revert(filesInDstores);

        //Updates the file allocation hash map of the instance
        this.fileAllocation.clear();
        for(Pair<String,List<Integer>> pair : fileAllocation){
            this.fileAllocation.put(pair.fst, shallowCopy(pair.snd));
        }

        //The hash map fileSizes of the instance needs to be updated as there may be files not existing anymore
        synchronized (this.fileSizes) {
            var iter = this.fileSizes.keySet().iterator();
            while(iter.hasNext()){
                var file = iter.next();
                if (!this.fileAllocation.containsKey(file))
                    iter.remove();
            }
        }

        var filesInDstoresImmutable = filesInDstores.stream()
                .map(pair -> new Pair<>(pair.fst,shallowCopy(pair.snd)))
                .collect(Collectors.toList());

        if(fileAllocation.size() != 0) {

            var informationForDstores = replicateFiles(filesInDstoresImmutable,filesInDstores, fileAllocation);
            informationForDstores = rebalance(fileAllocation, filesInDstoresImmutable,filesInDstores, informationForDstores);

            if(informationForDstores.size() == 0){
                alreadyRebalancing.set(false);
                return;
            }

            rebalancesCompleted.fst.fst.set(informationForDstores.size());
            rebalancesCompleted.fst.snd.set(0);
            rebalancesCompleted.snd = informationForDstores;
            sendRebalances(informationForDstores);


            var informationForDstoresCopy = informationForDstores;

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.schedule(()->{
                if(rebalancesCompleted.fst.fst.get() != 0){
                    System.out.println("ERROR => not all rebalance completed ackowledges have been received prior to the timeout");
                    rebalancesCompleted.fst.fst.set(0);
                    rebalancesCompleted.fst.snd.set(0);
                    updateFileAlocationAfterRebalance(informationForDstoresCopy);
                    alreadyRebalancing.set(false);
                }
            },timeout,TimeUnit.MILLISECONDS);


        }else{
            this.fileAllocation.clear();
            this.fileSizes.clear();
            alreadyRebalancing.set(false);
        }

    }


    private List<Pair<String,List<Integer>>> revert(List<Pair<Integer,List<String>>> filesInDstores){

        List<Pair<String,List<Integer>>> fileAllocation = new ArrayList<>();
        HashMap<String,List<Integer>> fileAllocationMap = new HashMap<>();

        for(Pair<Integer,List<String>> pair : filesInDstores) {

            int port = pair.fst;
            List<String> files = pair.snd;

            if(files.size()==1 && files.get(0).equals(""))
                continue;

            for (String file : files) {
                if (fileAllocationMap.containsKey(file)) {
                    fileAllocationMap.get(file).add(port);
                } else {
                    fileAllocationMap.put(file, new ArrayList<>(){{add(port);}});
                }
            }

        }

        for(String file : fileAllocationMap.keySet()){
            fileAllocation.add(new Pair<>(file,fileAllocationMap.get(file)));
        }
        return fileAllocation;
    }

    private HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> replicateFiles(List<Pair<Integer,List<String>>> filesInDstoresImmutable, List<Pair<Integer,List<String>>> filesInDstores, List<Pair<String,List<Integer>>> fileAllocation){

        //the first element of the pair is the file and the second is the number of times it needs to be replicated
        List<Pair<String,Integer>> filesNotRReplicatedNumber = fileAllocation.stream()
                .filter(file-> file.snd.size() < replicationFactor)
                .map(file -> new Pair<>(file.fst,replicationFactor - file.snd.size()))
                .collect(Collectors.toList());

        return replicateFilesAuxiliary(filesInDstoresImmutable,filesInDstores,filesNotRReplicatedNumber);
    }

    private HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> replicateFilesAuxiliary(List<Pair<Integer,List<String>>> filesInDstoreImmutable, List<Pair<Integer,List<String>>> filesInDstore, List<Pair<String,Integer>> filesNotRReplicatedNumber){

        HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> informationPerDstores = new HashMap<>();

        for(Pair<String,Integer> pair : filesNotRReplicatedNumber){
            String file = pair.fst;
            for(int i = 1; i <= pair.snd; i++){

                var dStorePortsOrderByNumFiles = filesInDstore.stream()
                        .map(p -> {
                            if (p.snd.get(0).equals(""))
                                return new Pair<>(p.fst, 0);
                            else
                                return new Pair<>(p.fst, p.snd.size());
                        })
                        .sorted(Comparator.comparingInt(p -> p.snd))
                        .map(p -> p.fst)
                        .collect(Collectors.toList());

                for(int n = 0; n < dStorePortsOrderByNumFiles.size(); n++){

                    int receivingPort = dStorePortsOrderByNumFiles.get(n);
                    int indexReceivingPort = filesInDstore.stream().map(p->p.fst).collect(Collectors.toList()).indexOf(receivingPort);
                    if(! filesInDstore.get(indexReceivingPort).snd.contains(file)){

                        int portTransferingFile = filesInDstoreImmutable.stream() //filesInDstoreImmutable needs to be used here as it contains the real current files that each dstore stores
                                .filter(par -> par.snd.contains(file))
                                .map(par -> par.fst)
                                .limit(1)
                                .collect(Collectors.toList())
                                .get(0);

                        filesInDstore.get(indexReceivingPort).snd.remove("");
                        filesInDstore.get(indexReceivingPort).snd.add(0,file);

                        if(informationPerDstores.containsKey(portTransferingFile)){
                            List<Pair<String,List<Integer>>> list = informationPerDstores.get(portTransferingFile).fst;
                            int indexFile = list.stream()
                                    .map(par -> par.fst )
                                    .collect(Collectors.toList())
                                    .indexOf(file);
                            if(indexFile != -1){
                                list.get(indexFile).snd.add(receivingPort);
                            }else{
                                list.add(new Pair<>(file,new ArrayList<>(){{add(receivingPort);}}));
                            }

                        }else{
                            informationPerDstores.put(portTransferingFile,new Pair<>(new ArrayList<>(){{add(new Pair<>(file,new ArrayList<>(){{add(receivingPort);}}));}},new ArrayList<>()));
                        }
                        break;
                    }

                }
            }
        }
        return informationPerDstores;
    }

    private HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> rebalance(List<Pair<String,List<Integer>>> fileAllocation,List<Pair<Integer,List<String>>> filesInDstoresImmutable ,List<Pair<Integer,List<String>>> filesInDstores, HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> informationForDstores){

        int numberFiles = fileAllocation.size();
        int numberDstores = filesInDstores.size();

        if((numberFiles * replicationFactor) >= numberDstores) {
            int filesPerDstore = (int) Math.floor(numberFiles * replicationFactor / (double) numberDstores);
            int[] expectedNumberFilesPerDstore = IntStream.generate(() -> filesPerDstore).limit(numberDstores).toArray();

            for (int i = 0; i < ((numberFiles * replicationFactor) % numberDstores); i++) {
                expectedNumberFilesPerDstore[i]++;
            }

            List<Pair<Integer, Integer>> dStorePortsOrderByNumFilesPair = filesInDstores.stream()
                    .map(pair -> {
                        if (pair.snd.get(0).equals("")) //TODO if a dstore has no files will it output an empty string?
                            return new Pair<>(pair.fst, 0);
                        else
                            return new Pair<>(pair.fst, pair.snd.size());
                    })
                    .sorted(Comparator.comparingInt(pair -> pair.snd))
                    .collect(Collectors.toList());

            Collections.reverse(dStorePortsOrderByNumFilesPair);

            List<Pair<Integer, Pair<Integer, Integer>>> dstoreNumberFilesExpected = new ArrayList<>();
            for (int i = 0; i < dStorePortsOrderByNumFilesPair.size(); i++) {
                int port = dStorePortsOrderByNumFilesPair.get(i).fst;
                int currentNumberFiles = dStorePortsOrderByNumFilesPair.get(i).snd;
                int expectedNumberFiles = expectedNumberFilesPerDstore[i];
                dstoreNumberFilesExpected.add(new Pair<>(port, new Pair<>(currentNumberFiles, expectedNumberFiles)));
            }

            return rebalanceAuxiliary(filesInDstoresImmutable, filesInDstores, dstoreNumberFilesExpected, informationForDstores);

        }else{

            List<Integer> expectedNumberFilesPerDstore = Stream.concat(
                    IntStream.generate(() -> 1).boxed().limit(numberFiles*replicationFactor),
                    IntStream.generate(()->0).boxed().limit(numberDstores-(numberFiles*replicationFactor))
            ).collect(Collectors.toList());


            List<Pair<Integer, Integer>> dStorePortsOrderByNumFilesPair = filesInDstores.stream()
                    .map(pair -> {
                        if (pair.snd.get(0).equals("")) //TODO if a dstore has no files will it output an empty string?
                            return new Pair<>(pair.fst, 0);
                        else
                            return new Pair<>(pair.fst, pair.snd.size());
                    })
                    .sorted(Comparator.comparingInt(pair -> pair.snd))
                    .collect(Collectors.toList());

            Collections.reverse(dStorePortsOrderByNumFilesPair);

            List<Pair<Integer, Pair<Integer, Integer>>> dstoreNumberFilesExpected = new ArrayList<>();
            for (int i = 0; i < dStorePortsOrderByNumFilesPair.size(); i++) {
                int port = dStorePortsOrderByNumFilesPair.get(i).fst;
                int currentNumberFiles = dStorePortsOrderByNumFilesPair.get(i).snd;
                int expectedNumberFiles = expectedNumberFilesPerDstore.get(i);
                dstoreNumberFilesExpected.add(new Pair<>(port, new Pair<>(currentNumberFiles, expectedNumberFiles)));
            }

            return rebalanceAuxiliary(filesInDstoresImmutable, filesInDstores, dstoreNumberFilesExpected, informationForDstores);
        }
    }

    // in the pair the first element is the port
    //the second element is another pair where the first element is the number of current elements and the second element is the expected number of elements
    private HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> rebalanceAuxiliary(List<Pair<Integer,List<String>>> filesInDstoresImmutable,List<Pair<Integer,List<String>>> filesInDstores, List<Pair<Integer,Pair<Integer,Integer>>> dstoreNumberFilesExpected, HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> informationForDstores){

        dstoreNumberFilesExpected = dstoreNumberFilesExpected.stream()
                .sorted(Comparator.comparingInt(pair -> pair.snd.fst - pair.snd.snd))
                .collect(Collectors.toList());

        Pair<Integer,Pair<Integer,Integer>> emptiestPort = dstoreNumberFilesExpected.get(0);
        int diff = emptiestPort.snd.fst - emptiestPort.snd.snd;

        if (diff == 0) //all ports have the right number of files stored
            return informationForDstores;
        else{
            for(int i = dstoreNumberFilesExpected.size()-1; i >= 0; i--){

                Pair<Integer,Pair<Integer,Integer>> portTransferingFilePair = dstoreNumberFilesExpected.get(i);
                if(portTransferingFilePair.snd.fst - portTransferingFilePair.snd.snd <= 0){
                    break; //all the other ports will have a diff which will be less than or equal to the diff above
                }

                int indexPortTrasfering = filesInDstores.stream()
                        .map(pair -> pair.fst)
                        .collect(Collectors.toList())
                        .indexOf(portTransferingFilePair.fst);

                int indexEmptiestPort = filesInDstores.stream()
                        .map(pair-> pair.fst)
                        .collect(Collectors.toList())
                        .indexOf(emptiestPort.fst);

                Iterator<String> iter = filesInDstores.get(indexPortTrasfering).snd.iterator();
                for(int n = 0; n < filesInDstores.get(indexPortTrasfering).snd.size(); n++){

                    String file = filesInDstores.get(indexPortTrasfering).snd.get(n);
                    //This if statement checks whether the dstore aiming to transfer the file actually has it stored
                    // and won't receive it later during the rebalance messages
                    if(filesInDstoresImmutable.get(indexPortTrasfering).snd.contains(file)) {

                        if (!filesInDstores.get(indexEmptiestPort).snd.contains(file)) {

                            filesInDstores.get(indexPortTrasfering).snd.remove(file);
                            portTransferingFilePair.snd = new Pair<>(portTransferingFilePair.snd.fst - 1, portTransferingFilePair.snd.snd);

                            filesInDstores.get(indexEmptiestPort).snd.add(file);
                            emptiestPort.snd = new Pair<>(emptiestPort.snd.fst + 1, emptiestPort.snd.snd);

                            updateInformationForDstores(portTransferingFilePair.fst, emptiestPort.fst, file, informationForDstores);

                            HashMap<Integer, Pair<List<Pair<String, List<Integer>>>, List<String>>> newInformationForDstores = rebalanceAuxiliary(filesInDstoresImmutable,filesInDstores, dstoreNumberFilesExpected, informationForDstores);

                            if (newInformationForDstores != null)
                                return newInformationForDstores;
                            else {
                                filesInDstores.get(indexPortTrasfering).snd.add(n, file);
                                portTransferingFilePair.snd = new Pair<>(portTransferingFilePair.snd.fst + 1, portTransferingFilePair.snd.snd);
                                filesInDstores.get(indexEmptiestPort).snd.remove(file);
                                emptiestPort.snd = new Pair<>(emptiestPort.snd.fst - 1, emptiestPort.snd.snd);
                                cancelUpdateInformationForDstores(portTransferingFilePair.fst, emptiestPort.fst, file, informationForDstores);
                            }
                        }
                    }
                }
            }
            return  null;
        }
    }

    private void updateInformationForDstores(int portTrasfering, int portReceving, String file, HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> informationForDstores){

        if(informationForDstores.containsKey(portTrasfering)){

            informationForDstores.get(portTrasfering).snd.add(file);
            int indexOfPair = informationForDstores.get(portTrasfering).fst.stream()
                    .map(pair -> pair.fst)
                    .collect(Collectors.toList())
                    .indexOf(file);

            if(indexOfPair != -1){
                informationForDstores.get(portTrasfering).fst.get(indexOfPair).snd.add(portReceving);

            }else{
                informationForDstores.get(portTrasfering).fst.add(new Pair<>(file,new ArrayList<>(){{add(portReceving);}}));
            }

        }else{
            informationForDstores.put(portTrasfering, new Pair<>(new ArrayList<>(){{add(new Pair<>(file,new ArrayList<>(){{add(portReceving);}}));}},new ArrayList<>(){{add(file);}}));
        }
    }

    private void cancelUpdateInformationForDstores(int portTrasfering, int portReceving, String file, HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> informationForDstores){

        informationForDstores.get(portTrasfering).snd.remove(file);

        int indexOfPair = informationForDstores.get(portTrasfering).fst.stream()
                .map(pair -> pair.fst)
                .collect(Collectors.toList())
                .indexOf(file);

        informationForDstores.get(portTrasfering).fst.get(indexOfPair).snd.remove((Integer) portReceving);
        if(informationForDstores.get(portTrasfering).fst.get(indexOfPair).snd.size() == 0){
            informationForDstores.get(portTrasfering).fst.remove(indexOfPair);
        }
    }

    private void sendRebalances(HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> informationForDstores){

        if(informationForDstores.size() != 0) {
            for (int port : informationForDstores.keySet()) {
                new Thread(() -> sendRebalance(port, informationForDstores.get(port))).start();
            }
        }
    }

    private void sendRebalance(int dstorePort, Pair<List<Pair<String,List<Integer>>>,List<String>> sendToRemove){

        try {
            Socket socket = dstorePortsSocket.get(dstorePort);
            PrintWriter writer = new PrintWriter(socket.getOutputStream());
            String message = rebalanceSentenceFormat(sendToRemove);
            writer.println(message);
            writer.flush();
            ControllerLogger.getInstance().messageSent(socket, message);

        }catch (IOException e){
            //TODO error handling
        }
    }

    private String rebalanceSentenceFormat(Pair<List<Pair<String,List<Integer>>>,List<String>> sendToRemove){
        int numberFilesToSend = sendToRemove.fst.size();

        String filesToSend = sendToRemove.fst.stream()
                .map(pair -> String.format("%s %s %s",pair.fst,pair.snd.size(),pair.snd.stream().map(Objects::toString).collect(Collectors.joining(" "))))
                .collect(Collectors.joining(" "));
        int numberFilesToRemove = sendToRemove.snd.size();
        String filesToRemove = String.join(" ", sendToRemove.snd);

        return String.format("%s %s %s %s %s",DstoreCommands.REBALANCE.toString(),numberFilesToSend,filesToSend,numberFilesToRemove,filesToRemove);
    }

    private void updateFileAlocationAfterRebalance(HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> informationForDstores){
        List<List<Pair<String,List<Integer>>>> fileAdditions = new ArrayList<>();
        List<Pair<Integer,List<String>>> portFilesToRemovePairs = new ArrayList<>();

        for(int port : informationForDstores.keySet()){
            fileAdditions.add(informationForDstores.get(port).fst);
            portFilesToRemovePairs.add(new Pair<>(port,informationForDstores.get(port).snd));
        }

        updateFileAllocationAdditions(fileAdditions);
        updateFileAllocationRemovesMultiplePorts(portFilesToRemovePairs);
    }

    private void updateFileAllocationAdditions(List<List<Pair<String,List<Integer>>>> fileAdditions){
        for(var list : fileAdditions){
            for(var pair : list){
                if(this.fileAllocation.containsKey(pair.fst)) {
                    this.fileAllocation.get(pair.fst).addAll(pair.snd.stream().filter(dstorePorts::contains).collect(Collectors.toList()));
                }
            }
        }
    }

    private void updateFileAllocationRemovesMultiplePorts(List<Pair<Integer,List<String>>> portFilesToRemovePairs){
        for(var portFilesToRemove : portFilesToRemovePairs){
            updateFileAllocationRemovesOnePort(portFilesToRemove);
        }

    }

    private void updateFileAllocationRemovesOnePort(Pair<Integer,List<String>> portFilesToRemove){
        for(String file : portFilesToRemove.snd) {
            if (this.fileAllocation.containsKey(file)){
                this.fileAllocation.get(file).remove(portFilesToRemove.fst);
            }
        }
    }

    private <T> List<T> shallowCopy(List<T> list){
        List<T> listCopy = new ArrayList<T>();
        listCopy.addAll(list);
        return listCopy;
    }


    /*************************************** END OF REBALANCE PROCEDURES *******************************************/

    /************************************BEGINNING OF HANDLE REBALANCE COMPLETE ***********************************/

    private void handleRebalanceComplete(){
        if(rebalancesCompleted.fst.fst.get() != 0){
            if(rebalancesCompleted.fst.fst.get() == rebalancesCompleted.fst.snd.incrementAndGet()){
                updateFileAlocationAfterRebalance(rebalancesCompleted.snd);
                rebalancesCompleted.fst.fst.set(0);
                rebalancesCompleted.fst.snd.set(0);
                rebalancesCompleted.snd = null;
                alreadyRebalancing.set(false);
            }
        }else{
            System.out.println("ERROR => a rebalance ackowledge has been received too late");
        }
    }

    /***************************************END OF HANDLE REBALANCE COMPLETE **************************************/

    /*************************************** BEGINNING OF COMMAND RECOGNISER PROCEDURES *******************************************/

    private ControllerCommands commandRecogniser(byte[] array, int length){

        String wholeSentence = new String(array,0,length);
        int firstSpaceIndex = wholeSentence.indexOf(" ");
        String command = wholeSentence.substring(0,firstSpaceIndex);

        if (command.equals(ControllerCommands.STORE.toString()))
            return ControllerCommands.STORE;
        else if (command.equals(ControllerCommands.STORE_ACK.toString()))
            return ControllerCommands.STORE_ACK;
        else if (command.equals(ControllerCommands.LOAD.toString()))
            return ControllerCommands.LOAD;
        else if (command.equals(ControllerCommands.RELOAD.toString()))
            return ControllerCommands.RELOAD;
        else if (command.equals(ControllerCommands.REMOVE.toString()))
            return ControllerCommands.REMOVE;
        else if (command.equals(ControllerCommands.REMOVE_ACK.toString()))
            return ControllerCommands.REMOVE_ACK;
        else if (command.equals(ControllerCommands.ERROR_FILE_DOES_NOT_EXIST.toString()))
            return ControllerCommands.ERROR_FILE_DOES_NOT_EXIST;
        else if (command.equals(ControllerCommands.LIST.toString()))
            return ControllerCommands.LIST;
        else if (command.equals(ControllerCommands.JOIN.toString()))
            return ControllerCommands.JOIN;
        else if (command.equals(ControllerCommands.REBALANCE_COMPLETE.toString()))
            return ControllerCommands.REBALANCE_COMPLETE;
        else
            return ControllerCommands.NoCommand;

    }

    private ControllerCommands commandRecogniser(String sentence){

        String[] splitted = sentence.split(" ");
        String command = splitted[0];

        if (command.equals(ControllerCommands.STORE.toString()))
            return ControllerCommands.STORE;
        else if (command.equals(ControllerCommands.STORE_ACK.toString()))
            return ControllerCommands.STORE_ACK;
        else if (command.equals(ControllerCommands.LOAD.toString()))
            return ControllerCommands.LOAD;
        else if (command.equals(ControllerCommands.RELOAD.toString()))
            return ControllerCommands.RELOAD;
        else if (command.equals(ControllerCommands.REMOVE.toString()))
            return ControllerCommands.REMOVE;
        else if (command.equals(ControllerCommands.REMOVE_ACK.toString()))
            return ControllerCommands.REMOVE_ACK;
        else if (command.equals(ControllerCommands.ERROR_FILE_DOES_NOT_EXIST.toString()))
            return ControllerCommands.ERROR_FILE_DOES_NOT_EXIST;
        else if (command.equals(ControllerCommands.LIST.toString()))
            return ControllerCommands.LIST;
        else if (command.equals(ControllerCommands.JOIN.toString()))
            return ControllerCommands.JOIN;
        else if (command.equals(ControllerCommands.REBALANCE_COMPLETE.toString()))
            return ControllerCommands.REBALANCE_COMPLETE;
        else
            return ControllerCommands.NoCommand;
    }

    /*************************************** END OF COMMAND RECOGNISER PROCEDURES *******************************************/

    // in the pair the first element is the port
    //the second element is another pair where the first element is the number of current elements and the second element is the expected number of elements
    /*private HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> rebalanceAuxiliary(List<Pair<Integer,List<String>>> filesInDstores, List<Pair<Integer,Pair<Integer,Integer>>> dstoreNumberFilesExpected, HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> informationForDstores){

        dstoreNumberFilesExpected = dstoreNumberFilesExpected.stream()
                .sorted(Comparator.comparingInt(pair -> pair.snd.fst - pair.snd.snd))
                .collect(Collectors.toList());

        Pair<Integer,Pair<Integer,Integer>> emptiestPort = dstoreNumberFilesExpected.get(0);
        int diff = emptiestPort.snd.fst - emptiestPort.snd.snd;

        if (diff == 0)
            return informationForDstores;
        else{
            for(int i = dstoreNumberFilesExpected.size()-1; i >= 0; i--){

                Pair<Integer,Pair<Integer,Integer>> portTransferingFilePair = dstoreNumberFilesExpected.get(i);

                int indexPortTrasfering = filesInDstores.stream()
                        .map(pair -> pair.fst)
                        .collect(Collectors.toList())
                        .indexOf(portTransferingFilePair.fst);

                int indexEmptiestPort = filesInDstores.stream()
                        .map(pair-> pair.fst)
                        .collect(Collectors.toList())
                        .indexOf(emptiestPort.fst);

                for(String file : filesInDstores.get(indexPortTrasfering).snd){

                    if(! filesInDstores.get(indexEmptiestPort).snd.contains(file)){

                        int indexFile = filesInDstores.get(indexPortTrasfering).snd.indexOf(file);
                        boolean toBeRemoved = (portTransferingFilePair.snd.fst - portTransferingFilePair.snd.snd) > 0;

                        if(toBeRemoved){
                            filesInDstores.get(indexPortTrasfering).snd.remove(file);
                            portTransferingFilePair.snd = new Pair<>(portTransferingFilePair.snd.fst-1,portTransferingFilePair.snd.snd);
                        }

                        filesInDstores.get(indexEmptiestPort).snd.add(file);
                        emptiestPort.snd = new Pair<>(emptiestPort.snd.fst+1, emptiestPort.snd.snd);

                        updateInformationForDstores(portTransferingFilePair.fst,emptiestPort.fst,toBeRemoved,file,informationForDstores);

                        HashMap<Integer,Pair<List<Pair<String,List<Integer>>>,List<String>>> newInformationForDstores = rebalanceAuxiliary(filesInDstores, dstoreNumberFilesExpected,informationForDstores);
                        if (newInformationForDstores != null)
                            return newInformationForDstores;
                        else{
                            if(toBeRemoved){
                                filesInDstores.get(indexPortTrasfering).snd.add(indexFile,file);
                                portTransferingFilePair.snd = new Pair<>(portTransferingFilePair.snd.fst+1,portTransferingFilePair.snd.snd);
                            }
                            filesInDstores.get(indexEmptiestPort).snd.remove(file);
                            emptiestPort.snd = new Pair<>(emptiestPort.snd.fst-1, emptiestPort.snd.snd);
                            cancelUpdateInformationForDstores(portTransferingFilePair.fst,emptiestPort.fst,toBeRemoved,file,informationForDstores);
                        }
                    }
                }
            }
            return  null;
        }
    }*/

    /***********************************************BEGINNING OF GETTERS AND SETTERS********************************************/

    public int getPort() {
        return port;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getRebalancePeriod() {
        return rebalancePeriod;
    }

    public List<Integer> getDstorePorts() {
        return dstorePorts;
    }

    public Map<String, List<Integer>> getFileAllocation() {
        return fileAllocation;
    }

    public Map<String, Long> getFileSizes() {
        return fileSizes;
    }

    public Map<String, Pair<AtomicInteger, Pair<Socket, Pair<Long, List<Integer>>>>> getAcknowledges() {
        return acknowledges;
    }

    public void setCanRebalance(boolean canRebalance) {
        this.canRebalance = canRebalance;
    }

    public Map<Integer, Socket> getDstorePortsSocket() {
        return dstorePortsSocket;
    }

    public Pair<List<Integer>, List<Pair<Integer, List<String>>>> getFilesIndStore() {
        return filesIndStore;
    }

    public Pair<Pair<AtomicInteger, AtomicInteger>, HashMap<Integer, Pair<List<Pair<String, List<Integer>>>, List<String>>>> getRebalancesCompleted() {
        return rebalancesCompleted;
    }

    public Map<Socket, List<Integer>> getSuspendedReloads() {
        return suspendedReloads;
    }

    public AtomicBoolean getRemovingOperation() {
        return removingOperation;
    }

    public AtomicBoolean getStoringOperation() {
        return storingOperation;
    }

    public boolean isCanRebalance() {
        return canRebalance;
    }



    /********************************************START FOR TESTING*****************************************/
    public void start(){
        new Thread(this::scheduleRebalanceOperation).start();
        new Thread(this::waitForRequestsFromClients).start();
    }


    /***************************************************END OF GETTERS AND SETTERS**********************************************/
    public static void main(String[] args) {
        Controller controller = new Controller(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]),Integer.parseInt(args[3]));
        controller.start();
    }

}
