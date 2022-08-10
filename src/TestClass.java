import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Assertions;

import java.io.*;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestClass {

    private Controller controller;
    private Thread controllerThread;
    private int controllerPort;
    private final int REPLICATION_FACTOR = 2;
    private final int TIME_OUT = 300000000;
    private final int REBALANCE_PERIOD = 300000000;

    @BeforeAll
    void set(){
        setAuxiliary(REPLICATION_FACTOR,TIME_OUT,REBALANCE_PERIOD);
    }

    void setAuxiliary(int replicationFactor, int timeout,int rebalancePeriod){
        Random random = new Random();
        List<Integer> freePorts = listFreePorts();
        controllerPort = freePorts.get(random.nextInt(freePorts.size()));
        controller = new Controller(controllerPort,replicationFactor,timeout, rebalancePeriod);
        controller.setCanRebalance(false);
        controllerThread = new Thread(()-> controller.start());
        controllerThread.start();
    }

    @AfterEach
    void resetController() throws InterruptedException{
        controllerThread.interrupt();
        controllerPort = listFreePorts().get(0);
        ControllerLogger.resetInstance();
        controller = new Controller(controllerPort,REPLICATION_FACTOR,TIME_OUT,REBALANCE_PERIOD);
        controller.setCanRebalance(false);
        controllerThread = new Thread(()-> controller.start());
        controllerThread.start();
        Thread.sleep(3000);
    }

    void resetController(int replicationFactor, int timeout, int rebalacePeriod) throws InterruptedException{
        controllerThread.interrupt();
        ControllerLogger.resetInstance();
        controllerPort = listFreePorts().get(0);
        controller = new Controller(controllerPort,replicationFactor,timeout,rebalacePeriod);
        controller.setCanRebalance(false);
        controllerThread = new Thread(()-> controller.start());
        controllerThread.start();
        Thread.sleep(3000);
    }

    /************************************BEGINNING OF TESTS FOR JOIN OPERATIONS********************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestJoinOperation")
    void testJoinOperation(int n) throws InterruptedException, IOException {
        var pair = createDStoreProcessesWithFolders(n);
        Thread.sleep(3000);
        for(int i : pair.fst){
            Assertions.assertEquals(1, controller.getDstorePorts().stream().filter(ds -> ds == i).count());
        }
        destroyProcesses(pair.snd);
        deleteFolders(pair.fst);
    }

    Stream<Arguments> argumentsForTestJoinOperation(){
        return Stream.of(
          Arguments.of(1),
          Arguments.of(2),
          Arguments.of(3),
          Arguments.of(10)
        );
    }

    /***************************************END OF TESTS FOR JOIN OPERATIONS************************************/


    /***************************************BEGINNING OF  TESTS FOR LIST OPERATIONS****************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestListOperation")
    void testListOperation(List<String> files) throws InterruptedException, IOException {

        var pair = createDStoreProcessesWithFolders(2);
        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();


        Thread.sleep(3000);

        for (String fileName : files) {
            File file = new File(fileName);
            file.createNewFile();
            client.store(file);
        }

        Thread.sleep(3000);
        String[] list = client.list();


        List<String> answer = Arrays.stream(list).sorted().collect(Collectors.toList());
        Collections.sort(files);
        Assertions.assertLinesMatch(files,answer);

        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForTestListOperation () {
        return Stream.of(
                Arguments.of(new ArrayList<>()),
                Arguments.of(new ArrayList<>(){{add("hello.txt");}}),
                Arguments.of(new ArrayList<>(){{add("hello.txt");add("hello2.txt");}}),
                Arguments.of(new ArrayList<>(){{add("hello.txt");add("hello2.txt");add("hello3.txt");}}),
                Arguments.of(new ArrayList<>(){{add("ciao.png");add("ciao2.png");add("ciao3.png");}})
        );
    }

    /************************************END OF TESTS FOR LIST OPERATIONS********************************/


    /*********************************BEGINNING OF TESTS FOR LIST OPERATION FAILURES****************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestListOperationFailures")
    void testListOperationFailures(int numberDstores, int replicationFactor) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,REBALANCE_PERIOD);
        var pair = createDStoreProcessesWithFolders(numberDstores);
        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        Thread.sleep(3000);
        Assertions.assertThrows(NotEnoughDstoresException.class, client::list);
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForTestListOperationFailures(){
        return Stream.of(
                Arguments.of(2,10),
                Arguments.of(4,10),
                Arguments.of(2,3),
                Arguments.of(9,10),
                Arguments.of(19,20),
                Arguments.of(15,19)
        );
    }

    /*************************************END OF TESTS FOR LIST OPERATION FAILURES******************************/

    /**********************************BEGINNING OF TESTS FOR STORE OPERATIONS*****************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestStoreOperation")
    void testStoreOperation(List<Pair<String,String>> files, int numberDstores) throws InterruptedException, IOException {

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        for(var par : files){
            client.store(createFile(par.fst,par.snd));
        }

        Thread.sleep(3000);

        for(var par : files){
            int counter = 0;
            for(int port : pair.fst){
                File file = new File(String.format("port_%s",port));
                if (Arrays.stream(file.list()).collect(Collectors.toList()).contains(par.fst)){
                    Assertions.assertTrue(controller.getFileAllocation().get(par.fst).contains(port));
                    FileInputStream input = new FileInputStream(String.format("port_%s",port) + File.separator + par.fst);
                    String actualContent = new String(input.readAllBytes());
                    input.close();
                    Assertions.assertTrue((actualContent.equals(par.snd+ "\r\n")) || par.snd.equals(actualContent));
                    counter++;
                }
            }
            Assertions.assertEquals(controller.getReplicationFactor(),counter);
        }

        //Check whether the files are evenly distributed
        int numberFiles = controller.getFileAllocation().size();
        int dstoreNumber = controller.getDstorePorts().size();
        int min = (int) Math.floor(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        int max = (int) Math.ceil(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        for(int port : pair.fst){
            File file = new File(String.format("port_%s",port));
            Assertions.assertTrue(file.list().length == min || file.list().length == max);
        }

        deleteFolders(pair.fst);
        deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForTestStoreOperation(){
        return Stream.of(
                firstArgumentStoreOperation(),
                secondArgumentStoreOperation(),
                thirdArgumentStoreOperation(),
                fourthArgumentStoreOperation(),
                fifthArgumentStoreOperation()
        );
    }

    Arguments firstArgumentStoreOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,2);
    }

    Arguments secondArgumentStoreOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO\nHello, it is me"));
        }};

        return Arguments.of(files,2);
    }

    Arguments thirdArgumentStoreOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO\nHello, it is me"));
            add(new Pair<>("hello1.txt","You make me wanna freeze"));
        }};

        return Arguments.of(files,4);
    }

    Arguments fourthArgumentStoreOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO\nHello, it is me"));
            add(new Pair<>("hello1.txt","You make me wanna freeze"));
            add(new Pair<>("pen.txt","Pen"));
        }};

        return Arguments.of(files,5);
    }

    Arguments fifthArgumentStoreOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO\nHello, it is me"));
            add(new Pair<>("hello1.txt","You make me wanna freeze"));
            add(new Pair<>("pen.txt","Pen"));
            add(new Pair<>("penna.txt","Pennaaaaa"));
            add(new Pair<>("bel.txt","Gingles everywhere"));
            add(new Pair<>("malm.txt","Malmo VS Pan"));
            add(new Pair<>("dog.txt","Mustard is yellow"));
            add(new Pair<>("sin.txt","Collateral beauty"));
            add(new Pair<>("benny.txt","Bennnnnnnnnny\nBenny\n\nBenny"));
        }};

        return Arguments.of(files,5);
    }

    File createFile(String fileName,String content) throws IOException {
        File file = new File(fileName);
        file.createNewFile();
        PrintWriter writer = new PrintWriter(new FileOutputStream(file));
        writer.println(content);
        writer.close();
        return file;
    }

    /***************************************BEGINNING OF TESTS FOR STORE TIMEOUT**********************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestStoreTimeoutOperation")
    void testStoreTimeoutOperation(int numberDstores, int replicationFactor, int timeout) throws InterruptedException, IOException {
        resetController(replicationFactor,timeout,300);

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        try {
            client.store(createFile("hello.txt", "CIAO HELLO HALO"));
        }catch(Exception e){

        }

        Thread.sleep(300);
        Assertions.assertFalse(controller.getAcknowledges().containsKey("hello.txt"));

        deleteFolders(pair.fst);
        deleteFiles(new ArrayList<>(){{add("hello.txt");}});
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForTestStoreTimeoutOperation(){
        return Stream.of(
                Arguments.of(3,3,1),
                Arguments.of(10,10,15)
        );
    }

    /***************************************END OF TESTS FOR STORE TIMEOUT**********************************/

    /******************************BEGINNING OF TESTS FOR STORE OPERATIONS FAILURES*************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestStoreOperationFailure")
    void testStoreOperationFailures(List<Pair<String,String>> files, int numberDstores, int replicationFactor) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,REBALANCE_PERIOD);

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();
        boolean exceptionThrown = false;

        try {

            for (var par : files) {
                client.store(createFile(par.fst, par.snd));
            }

            Assertions.assertTrue(false);

            deleteFolders(pair.fst);
            deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
            destroyProcesses(pair.snd);

        }catch(NotEnoughDstoresException | FileAlreadyExistsException e){

            Assertions.assertTrue(true);

            deleteFolders(pair.fst);
            deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
            destroyProcesses(pair.snd);
        }

    }

    Stream<Arguments> argumentsForTestStoreOperationFailure(){
        return Stream.of(
                firstArgumentStoreOperationFailure(),
                secondArgumentStoreOperationFailure(),
                thirdArgumentStoreOperationFailure(),
                fourthArgumentStoreOperationFailure(),
                fifthArgumentStoreOperationFailure()
        );
    }

    Arguments firstArgumentStoreOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,2,3);
    }

    Arguments secondArgumentStoreOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,3,5);
    }

    Arguments thirdArgumentStoreOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("hello.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,5,2);
    }

    Arguments fourthArgumentStoreOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("ben.txt","CIAO SONO IO"));
            add(new Pair<>("benny.txt","CIAO SONO IO"));
            add(new Pair<>("been.txt","CIAO SONO IO"));
            add(new Pair<>("hello.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,3,1);
    }

    Arguments fifthArgumentStoreOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("ben.txt","CIAO SONO IO"));
            add(new Pair<>("benny.txt","CIAO SONO IO"));
            add(new Pair<>("been.txt","CIAO SONO IO"));
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("beetn.txt","CIAO SONO IO"));
            add(new Pair<>("benton.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,8,5);
    }


    /***********************************END OF TESTS FOR STORE OPERATIONS FAILURES*************************/


    /***********************************BEGINNING OF TESTS DSTORE FAILS***********************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestDStoreFailures")
    void testDStoreFailures(int numberDstores, int numberFailures) throws InterruptedException, IOException {

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);
        destroyProcesses(pair.snd.stream().limit(numberFailures).collect(Collectors.toList()));
        Thread.sleep(1000);

        List<Integer> failedPorts = pair.fst.stream().limit(numberFailures).collect(Collectors.toList());
        List<Integer> activePorts = pair.fst.stream().skip(numberFailures).collect(Collectors.toList());

        for(int port : failedPorts){
            Assertions.assertFalse(controller.getDstorePorts().contains(port));
        }

        for(int port : activePorts){
            Assertions.assertTrue(controller.getDstorePorts().contains(port));
        }

        destroyProcesses(pair.snd);
        deleteFolders(pair.fst);
    }

    Stream<Arguments> argumentsForTestDStoreFailures(){
        return Stream.of(
                Arguments.of(5,3),
                Arguments.of(5,5),
                Arguments.of(3,0),
                Arguments.of(10,1),
                Arguments.of(10,8)
        );
    }

    /**************************************END OF TESTS DSTORE FAILS***********************************/


    /*********************************BEGINNING OF TESTS FOR LOAD OPERATION****************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestLoadOperation")
    void testLoadOperation(List<Pair<String,String>> files, String fileToLoad, int numberDstores, int replicationFactor) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,REBALANCE_PERIOD);

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        for(var par : files){
            client.store(createFile(par.fst,par.snd));
        }

        Thread.sleep(3000);

        String expectedContent = files.stream().filter(p -> p.fst.equals(fileToLoad)).map(p -> p.snd).limit(1).collect(Collectors.toList()).get(0);
        String actualContent = new String(client.load(fileToLoad));
        Assertions.assertTrue(actualContent.equals(expectedContent + "\r\n") || actualContent.equals(expectedContent));

        deleteFolders(pair.fst);
        deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForTestLoadOperation(){
        return Stream.of(
                firstArgumentLoadOperation(),
                secondArgumentLoadOperation(),
                thirdArgumentLoadOperation(),
                fourthArgumentLoadOperation(),
                fifthArgumentLoadOperation()
        );
    }

    Arguments firstArgumentLoadOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,"hello.txt",4,2);
    }

    Arguments secondArgumentLoadOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO BELLA"));
        }};

        return Arguments.of(files,"hello1.txt",5,4);
    }

    Arguments thirdArgumentLoadOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO CARO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO BELLA"));
        }};

        return Arguments.of(files,"hello.txt",5,4);
    }

    Arguments fourthArgumentLoadOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO CARO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO BELLA"));
            add(new Pair<>("helo.txt","helo.txt"));
            add(new Pair<>("mello1.txt","mello1.txt"));
            add(new Pair<>("pin.txt","pinPin belli pin"));
            add(new Pair<>("super.txt","SUPER"));
        }};

        return Arguments.of(files,"mello1.txt",2,1);
    }

    Arguments fifthArgumentLoadOperation(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO CARO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO BELLA"));
            add(new Pair<>("helo.txt","helo.txt"));
            add(new Pair<>("mello1.txt","mello1.txt"));
            add(new Pair<>("pin.txt","pinPin belli pin"));
            add(new Pair<>("super.txt","SUPER"));
        }};

        return Arguments.of(files,"pin.txt",10,7);
    }

    /***********************************END OF TESTS FOR LOAD OPERATION********************************/

    /***************************BEGINNING OF TESTS FOR LOAD OPERATION FAILURE**************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestLoadOperationFailure")
    void testLoadOperationFailure(List<Pair<String,String>> files, String fileToLoad, int numberDstores, int replicationFactor) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,REBALANCE_PERIOD);

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        if(numberDstores < replicationFactor){
            Assertions.assertThrows(IOException.class,()->client.load(fileToLoad));
            deleteFolders(pair.fst);
            deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
            destroyProcesses(pair.snd);
        }else {

            for(var par : files){
                client.store(createFile(par.fst,par.snd));
            }

            Thread.sleep(3000);

            try {
                client.load(fileToLoad);
                deleteFolders(pair.fst);
                deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
                destroyProcesses(pair.snd);
                Assertions.assertTrue(false);

            } catch (NotEnoughDstoresException | FileDoesNotExistException e) {
                deleteFolders(pair.fst);
                deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
                destroyProcesses(pair.snd);
                Assertions.assertTrue(true);
            }
        }
    }

    Stream<Arguments> argumentsForTestLoadOperationFailure() {
        return Stream.of(
                firstArgumentLoadOperationFailure(),
                secondArgumentLoadOperationFailure(),
                thirdArgumentLoadOperationFailure(),
                fourthArgumentLoadOperationFailure(),
                fifthArgumentLoadOperationFailure()
        );
    }

    Arguments firstArgumentLoadOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,"hello.txt",2,4);
    }

    Arguments secondArgumentLoadOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO BELLA"));
        }};

        return Arguments.of(files,"hello1.txt",5,10);
    }

    Arguments thirdArgumentLoadOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO CARO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO BELLA"));
        }};

        return Arguments.of(files,"hellop.txt",5,4);
    }

    Arguments fourthArgumentLoadOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO CARO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO BELLA"));
            add(new Pair<>("helo.txt","helo.txt"));
            add(new Pair<>("mello1.txt","mello1.txt"));
            add(new Pair<>("pin.txt","pinPin belli pin"));
            add(new Pair<>("super.txt","SUPER"));
        }};

        return Arguments.of(files,"mello11.txt",2,1);
    }

    Arguments fifthArgumentLoadOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO CARO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO BELLA"));
            add(new Pair<>("helo.txt","helo.txt"));
            add(new Pair<>("mello1.txt","mello1.txt"));
            add(new Pair<>("pin.txt","pinPin belli pin"));
            add(new Pair<>("super.txt","SUPER"));
        }};

        return Arguments.of(files,"pinnn.txt",10,7);
    }

    /******************************END OF TESTS FOR LOAD OPERATION FAILURE*****************************/

    /*****************************BEGINNING OF TESTS FOR RELOAD OPERATION *******************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestReloadOperation")
    void testReloadOperation(List<Pair<String,String>> files, String fileToLoad, int numberDstores, int replicationFactor) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,REBALANCE_PERIOD);

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        for(var par : files){
            client.store(createFile(par.fst,par.snd));
        }

        Thread.sleep(3000);
        controller.getFileAllocation().put(fileToLoad,pair.fst);

        for(String file : controller.getFileAllocation().keySet()){
            Collections.shuffle(controller.getFileAllocation().get(file));
        }

        String expectedContent = files.stream().filter(p -> p.fst.equals(fileToLoad)).map(p -> p.snd).collect(Collectors.toList()).get(0);
        String actualContent = new String(client.load(fileToLoad));

        Assertions.assertTrue(actualContent.equals(expectedContent + "\r\n") || actualContent.equals(expectedContent));

        deleteFolders(pair.fst);
        deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForTestReloadOperation(){
        return Stream.of(
                firstArgumentReloadOperation(),
                secondArgumentReloadOperation(),
                thirdArgumentReloadOperation()
        );
    }

    Arguments firstArgumentReloadOperation(){
        List<Pair<String,String>> files = new ArrayList<>(){{
           add(new Pair<>("hello.txt","CIAO SONO IO"));
        }};
        return Arguments.of(files,"hello.txt",10,2);
    }

    Arguments secondArgumentReloadOperation(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO"));
        }};
        return Arguments.of(files,"hello1.txt",15,1);
    }

    Arguments thirdArgumentReloadOperation(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO"));
            add(new Pair<>("hello2.txt","GABI"));
            add(new Pair<>("hello3.txt","MENNI"));
        }};
        return Arguments.of(files,"hello2.txt",20,3);
    }

    /********************************END OF TESTS FOR RELOAD OPERATION *******************************/


    /***************************BEGINNING OF TESTS FOR RELOAD OPERATION ERROR LOAD****************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestReloadOperationFailureErrorLoad")
    void testReloadOperationFailureErrorLoad(String file, int numberDstores) throws InterruptedException, IOException {

        var pair = createDStoreProcesses(numberDstores);

        Thread.sleep(3000);
        controller.getFileAllocation().put(file,pair.fst);
        controller.getFileSizes().put(file,13L);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        Assertions.assertThrows(IOException.class,()->client.load(file));
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForTestReloadOperationFailureErrorLoad(){
        return Stream.of(
                Arguments.of("hello.txt",3),
                Arguments.of("hell.txt",6),
                Arguments.of("hellllo.txt",2),
                Arguments.of("hellooo.txt",10)
        );
    }


    /*************************************END OF TESTS FOR RELOAD OPERATION ERROR LOAD*************************************/



    /*************************BEGINNING OF TESTS FOR REBALANCE OPERATIONS - DSTORES JOINING****************/

    @ParameterizedTest
    @MethodSource("argumentsForTestRebalanceOperationDstoreJoining")
    void testRebalanceOperationDstoreJoining(List<Pair<String,String>> files, int numberDstoresStarting, int numberDstoreAdd, int replicationFactor,int rebalancePeriod) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,rebalancePeriod);

        var pair = createDStoreProcessesWithFolders(numberDstoresStarting);

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        for(var par : files){
            client.store(createFile(par.fst,par.snd));
        }

        Thread.sleep(3000);

        var pair2 = createDStoreProcessesWithFolders(numberDstoreAdd);
        controller.setCanRebalance(true);

        Thread.sleep(3000);

        Thread.sleep(rebalancePeriod*files.size());

        var ports = Stream.concat(pair.fst.stream(),pair2.fst.stream()).collect(Collectors.toList());


        for(var par : files){
            int counter = 0;
            for(int port : ports){
                File file = new File(String.format("port_%s",port));
                if (Arrays.stream(file.list()).collect(Collectors.toList()).contains(par.fst)){
                    Assertions.assertTrue(controller.getFileAllocation().get(par.fst).contains(port));
                    FileInputStream input = new FileInputStream(String.format("port_%s",port) + File.separator + par.fst);
                    String actualContent = new String(input.readAllBytes());
                    input.close();
                    Assertions.assertTrue((actualContent.equals(par.snd+ "\r\n")) || par.snd.equals(actualContent));
                    counter++;
                }
            }
            Assertions.assertEquals(controller.getReplicationFactor(),counter);
        }

        Assertions.assertEquals(files.size(),controller.getFileAllocation().size());
        Assertions.assertEquals(files.size(),controller.getFileSizes().size());

        for(var file : controller.getFileAllocation().keySet()){
            Assertions.assertEquals(controller.getReplicationFactor(),controller.getFileAllocation().get(file).size());
        }

        //Check whether the files are evenly distributed
        int numberFiles = controller.getFileAllocation().size();
        Assertions.assertEquals(numberDstoresStarting + numberDstoreAdd,controller.getDstorePorts().size());
        int dstoreNumber = controller.getDstorePorts().size();
        int min = (int) Math.floor(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        int max = (int) Math.ceil(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        for(int port : ports){
            File file = new File(String.format("port_%s",port));
            Assertions.assertTrue(file.list().length == min || file.list().length == max);
        }

        deleteFolders(ports);
        deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
        destroyProcesses(pair.snd);
        destroyProcesses(pair2.snd);
    }

    Stream<Arguments> argumentsForTestRebalanceOperationDstoreJoining(){
        return Stream.of(
                firstArgumentRebalanceDstoreJoining(),
                secondArgumentRebalanceDstoreJoining(),
                thirdArgumentRebalanceDstoreJoining(),
                fourthArgumentRebalanceDstoreJoining(),
                fifthArgumentRebalanceDstoreJoining(),
                sixthArgumentRebalanceDstoreJoining(),
                seventhArgumentRebalanceDstoreJoining(),
                eigthArgumentRebalanceDstoreJoining(),
                ninthArgumentRebalanceDstoreJoining(),
                tenthArgumentRebalanceDstoreJoining(),
                eleventhArgumentRebalanceDstoreJoining(),
                twelthArgumentRebalanceDstoreJoining(),
                thirteenthArgumentRebalanceDstoreJoining(),
                fourthArgumentRebalanceDstoreJoining(),
                fifteenthArgumentRebalanceDstoreJoining(),
                sixteenthArgumentRebalanceDstoreJoining(),
                seventeenthArgumentRebalanceDstoreJoining(),
                eighteenthArgumentRebalanceDstoreJoining(),
                nineteenthArgumentRebalanceDstoreJoining(),
                twentiethArgumentRebalanceDstoreJoining(),
                twentyFirstArgumentRebalanceDstoreJoining(),
                twentySecondArgumentRebalanceDstoreJoining(),
                twentyThirdArgumentRebalanceDstoreJoining(),
                twentyFourthArgumentRebalanceDstoreJoining(),
                twentyFifthArgumentRebalanceDstoreJoining(),
                twentySixthArgumentRebalanceDstoreJoining(),
                twentySeventhArgumentRebalanceDstoreJoining(),
                twentyEighthArgumentRebalanceDstoreJoining()

        );
    }

    Arguments firstArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
        }};

        return Arguments.of(files,3,2,3,300);
    }

    Arguments secondArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
        }};

        return Arguments.of(files,4,4,4,300);
    }

    Arguments thirdArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
        }};

        return Arguments.of(files,4,4,2,300);
    }

    Arguments fourthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE AGAIN THERE"));
            add(new Pair<>("hello4.txt","HEY HEY JUST THERE"));
            add(new Pair<>("hello5.txt","HEY ONLY THERE"));
        }};

        return Arguments.of(files,4,4,2,300);
    }

    Arguments fifthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
        }};

        return Arguments.of(files,2,5,2,300);
    }

    Arguments sixthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
        }};

        return Arguments.of(files,4,3,1,300);
    }

    Arguments seventhArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE BASTARD"));
            add(new Pair<>("hello2.txt","HEY THERE BASTARD"));
        }};

        return Arguments.of(files,4,3,1,300);
    }

    Arguments eigthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE AGAIN THERE"));
            add(new Pair<>("hello4.txt","HEY HEY JUST THERE"));
            add(new Pair<>("hello5.txt","HEY ONLY THERE"));
        }};

        return Arguments.of(files,3,1,2,300);
    }

    Arguments ninthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE AGAIN THERE"));
            add(new Pair<>("hello4.txt","HEY HEY JUST THERE"));
            add(new Pair<>("hello5.txt","HEY ONLY THERE"));
        }};

        return Arguments.of(files,5,2,4,300);
    }

    Arguments tenthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE AGAIN THERE"));
            add(new Pair<>("hello4.txt","HEY HEY JUST THERE"));
            add(new Pair<>("hello5.txt","HEY ONLY THERE"));
            add(new Pair<>("hello6.txt","HEY THERE SO GOOD"));
            add(new Pair<>("hello7.txt","HEY SO SO GOOD"));
            add(new Pair<>("hello8.txt","HEY BENNY"));
            add(new Pair<>("hello9.txt","HALO THERE"));
            add(new Pair<>("hello10.txt","HALO GUTE NACHT"));
        }};

        return Arguments.of(files,5,4,3,300);
    }

    Arguments eleventhArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE AGAIN THERE"));
            add(new Pair<>("hello4.txt","HEY HEY JUST THERE"));
            add(new Pair<>("hello5.txt","HEY ONLY THERE"));
            add(new Pair<>("hello6.txt","HEY THERE SO GOOD"));
            add(new Pair<>("hello7.txt","HEY SO SO GOOD"));
            add(new Pair<>("hello8.txt","HEY BENNY"));
            add(new Pair<>("hello9.txt","HALO THERE"));
            add(new Pair<>("hello10.txt","HALO GUTE NACHT"));
        }};

        return Arguments.of(files,5,4,3,300);
    }

    Arguments twelthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE AGAIN THERE"));
            add(new Pair<>("hello4.txt","HEY HEY JUST THERE"));
            add(new Pair<>("hello5.txt","HEY ONLY THERE"));
            add(new Pair<>("hello6.txt","HEY THERE SO GOOD"));
            add(new Pair<>("hello7.txt","HEY SO SO GOOD"));
            add(new Pair<>("hello8.txt","HEY BENNY"));
            add(new Pair<>("hello9.txt","HALO THERE"));
            add(new Pair<>("hello10.txt","HALO GUTE NACHT"));
        }};

        return Arguments.of(files,6,2,6,300);
    }

    Arguments thirteenthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE AGAIN THERE"));
            add(new Pair<>("hello4.txt","HEY HEY JUST THERE"));
            add(new Pair<>("hello5.txt","HEY ONLY THERE"));
            add(new Pair<>("hello6.txt","HEY THERE SO GOOD"));
            add(new Pair<>("hello7.txt","HEY SO SO GOOD"));
            add(new Pair<>("hello8.txt","HEY BENNY"));
            add(new Pair<>("hello9.txt","HALO THERE"));
            add(new Pair<>("hello10.txt","HALO GUTE NACHT"));
        }};

        return Arguments.of(files,8,3,3,300);
    }

    Arguments fourteenthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE AGAIN THERE"));
            add(new Pair<>("hello4.txt","HEY HEY JUST THERE"));
            add(new Pair<>("hello5.txt","HEY ONLY THERE"));
            add(new Pair<>("hello6.txt","HEY THERE SO GOOD"));
            add(new Pair<>("hello7.txt","HEY SO SO GOOD"));
            add(new Pair<>("hello8.txt","HEY BENNY"));
            add(new Pair<>("hello9.txt","HALO THERE"));
            add(new Pair<>("hello10.txt","HALO GUTE NACHT"));
        }};

        return Arguments.of(files,4,1,2,300);
    }

    Arguments fifteenthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE AGAIN THERE"));
            add(new Pair<>("hello4.txt","HEY HEY JUST THERE"));
            add(new Pair<>("hello5.txt","HEY ONLY THERE"));
            add(new Pair<>("hello6.txt","HEY THERE SO GOOD"));
            add(new Pair<>("hello7.txt","HEY SO SO GOOD"));
            add(new Pair<>("hello8.txt","HEY BENNY"));
            add(new Pair<>("hello9.txt","HALO THERE"));
            add(new Pair<>("hello10.txt","HALO GUTE NACHT"));
        }};

        return Arguments.of(files,5,4,3,300);
    }

    Arguments sixteenthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 30; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,5,4,3,300);
    }

    Arguments seventeenthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 40; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,2,0,2,300);
    }

    Arguments eighteenthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 15; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,4,6,3,300);
    }

    Arguments nineteenthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 15; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,5,6,5,300);
    }

    Arguments twentiethArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 22; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,7,1,3,300);
    }

    Arguments twentyFirstArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 22; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,7,1,7,300);
    }

    Arguments twentySecondArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 43; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,1,0,1,300);
    }

    Arguments twentyThirdArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 43; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,10,4,4,300);
    }

    Arguments twentyFourthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 50; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,21,6,4,300);
    }

    Arguments twentyFifthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 50; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,21,6,11,300);
    }


    Arguments twentySixthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 5; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,20,6,4,300);
    }

    Arguments twentySeventhArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 5; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,13,6,4,300);
    }

    Arguments twentyEighthArgumentRebalanceDstoreJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 5; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,17,10,4,300);
    }


    /***************************END OF TESTS FOR REBALANCE OPERATIONS - DSTORE JOINING***************/

    /***************************BEGINNING OF TESTS FOR REBALANCE OPERATIONS - DSTORE FAILING***************/

    @ParameterizedTest
    @MethodSource("argumentsForTestRebalanceOperationDstoreFailing")
    void testRebalanceOperationDstoreFailing(List<Pair<String,String>> files, int numberDstoresStarting, int numberDstoreRemove, int replicationFactor,int rebalancePeriod) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,rebalancePeriod);

        var pair = createDStoreProcessesWithFolders(numberDstoresStarting);

        List<Pair<Integer,Process>> pairs = new ArrayList<>();
        for(int i = 0; i < pair.fst.size(); i++){
            pairs.add(new Pair<>(pair.fst.get(i),pair.snd.get(i)));
        }

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        for(var par : files){
            client.store(createFile(par.fst,par.snd));
        }

        Thread.sleep(3000);

        Collections.shuffle(pairs);
        pairs.stream().limit(numberDstoreRemove).forEach(p -> p.snd.destroy());
        var portsRemoved = pairs.stream().limit(numberDstoreRemove).map(p -> p.fst).collect(Collectors.toList());

        Thread.sleep(300);

        controller.setCanRebalance(true);
        Thread.sleep(rebalancePeriod*files.size());

        var ports = pairs.stream().skip(numberDstoreRemove).map(p -> p.fst).collect(Collectors.toList());

        for(var par : files.stream().filter(p -> controller.getFileAllocation().containsKey(p.fst)).collect(Collectors.toList())){

            int counter = 0;
            for(int port : ports){
                File file = new File(String.format("port_%s",port));
                if (Arrays.stream(file.list()).collect(Collectors.toList()).contains(par.fst)){
                    Assertions.assertTrue(controller.getFileAllocation().get(par.fst).contains(port));
                    FileInputStream input = new FileInputStream(String.format("port_%s",port) + File.separator + par.fst);
                    String actualContent = new String(input.readAllBytes());
                    input.close();
                    Assertions.assertTrue((actualContent.equals(par.snd+ "\r\n")) || par.snd.equals(actualContent));
                    counter++;
                }
            }
            Assertions.assertEquals(controller.getReplicationFactor(),counter);
        }


        for(var port : portsRemoved){
            Assertions.assertFalse(controller.getDstorePortsSocket().containsKey(port));
        }


        for(var file : controller.getFileAllocation().keySet()){
            Assertions.assertEquals(controller.getReplicationFactor(),controller.getFileAllocation().get(file).size());
        }

        //Check whether the files are evenly distributed
        int numberFiles = controller.getFileAllocation().size();
        Assertions.assertEquals(numberDstoresStarting - numberDstoreRemove,controller.getDstorePorts().size());
        int dstoreNumber = controller.getDstorePorts().size();
        int min = (int) Math.floor(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        int max = (int) Math.ceil(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        for(int port : ports){
            File file = new File(String.format("port_%s",port));
            Assertions.assertTrue(file.list().length == min || file.list().length == max);
        }

        deleteFolders(pair.fst);
        deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
        destroyProcesses(pair.snd);

    }

    Stream<Arguments> argumentsForTestRebalanceOperationDstoreFailing(){
        return Stream.of(
                firstArgumentRebalanceDstoreFailing(),
                secondArgumentRebalanceDstoreFailing(),
                thirdArgumentRebalanceDstoreFailing(),
                fourthArgumentRebalanceDstoreFailing(),
                fifthArgumentRebalanceDstoreFailing(),
                sixthArgumentRebalanceDstoreFailing(),
                seventhArgumentRebalanceDstoreFailing(),
                eighthArgumentRebalanceDstoreFailing(),
                ninthArgumentRebalanceDstoreFailing(),
                tenthArgumentRebalanceDstoreFailing(),
                eleventhArgumentRebalanceDstoreFailing(),
                twelthArgumentRebalanceDstoreFailing(),
                thirteenthArgumentRebalanceDstoreFailing(),
                fourteenthArgumentRebalanceDstoreFailing(),
                fifteenthArgumentRebalanceDstoreFailing(),
                sixteenthArgumentRebalanceDstoreFailing(),
                seventeenthArgumentRebalanceDstoreFailing()



        );
    }

    Arguments firstArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
        }};

        return Arguments.of(files,5,2,3,300);
    }

    Arguments secondArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
        }};

        return Arguments.of(files,8,4,4,300);
    }

    Arguments thirdArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
        }};

        return Arguments.of(files,8,4,2,300);
    }

    Arguments fourthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE THERE AGAIN"));
        }};

        return Arguments.of(files,10,4,3,300);
    }

    Arguments fifthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE THERE AGAIN"));
        }};

        return Arguments.of(files,10,7,3,300);
    }

    Arguments sixthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE THERE THERE AGAIN"));
        }};

        return Arguments.of(files,10,7,3,300);
    }

    Arguments seventhArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE THERE THERE AGAIN"));
        }};

        return Arguments.of(files,20,8,5,300);
    }

    Arguments eighthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE THERE AGAIN"));
        }};

        return Arguments.of(files,20,8,10,300);
    }

    Arguments ninthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE THERE THERE AGAIN"));
            add(new Pair<>("hello6.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello7.txt","HEY THERE THERE THERE AGAIN"));
            add(new Pair<>("hello8.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello9.txt","HEY THERE THERE THERE AGAIN"));
        }};

        return Arguments.of(files,20,5,10,300);
    }

    Arguments tenthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE THERE THERE AGAIN"));
            add(new Pair<>("hello6.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello7.txt","HEY THERE THERE THERE AGAIN"));
            add(new Pair<>("hello8.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello9.txt","HEY THERE THERE THERE AGAIN"));
        }};

        return Arguments.of(files,8,7,1,300);
    }

    Arguments eleventhArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE THERE THERE AGAIN"));
            add(new Pair<>("hello6.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello7.txt","HEY THERE THERE THERE AGAIN"));
            add(new Pair<>("hello8.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello9.txt","HEY THERE THERE THERE AGAIN"));
        }};

        return Arguments.of(files,8,3,1,300);
    }

    Arguments twelthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE"));
            add(new Pair<>("hello3.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE THERE THERE AGAIN"));
            add(new Pair<>("hello6.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello7.txt","HEY THERE THERE THERE AGAIN"));
            add(new Pair<>("hello8.txt","HEY THERE THERE AGAIN AGAIN"));
            add(new Pair<>("hello9.txt","HEY THERE THERE THERE AGAIN"));
        }};

        return Arguments.of(files,8,3,2,300);
    }


    Arguments thirteenthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 40; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,10,1,3,300);
    }

    Arguments fourteenthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 42; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,3,1,1,300);
    }

    Arguments fifteenthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 25; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,10,1,8,300);
    }

    Arguments sixteenthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 30; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,10,1,1,300);
    }

    Arguments seventeenthArgumentRebalanceDstoreFailing(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 11; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,10,3,5,300);
    }


    /******************************END OF TESTS FOR REBALANCE OPERATIONS - DSTORE FAILING******************/

    /******************************BEGINNING OF TESTS FOR REBALANCE OPERATIONS - DSTORE FAILING AND JOINING **************/

    @ParameterizedTest
    @MethodSource("argumentsForTestRebalanceOperationDstoreFailingJoining")
    void testRebalanceOperationDstoreFailingJoining(List<Pair<String,String>> files, int numberDstoresStarting, int numberDstoreRemove, int numberDstoreJoining, int replicationFactor,int rebalancePeriod) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,rebalancePeriod);

        var pair = createDStoreProcessesWithFolders(numberDstoresStarting);

        List<Pair<Integer,Process>> pairs = new ArrayList<>();
        for(int i = 0; i < pair.fst.size(); i++){
            pairs.add(new Pair<>(pair.fst.get(i),pair.snd.get(i)));
        }

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        for(var par : files){
            client.store(createFile(par.fst,par.snd));
        }

        Thread.sleep(3000);

        Collections.shuffle(pairs);
        pairs.stream().limit(numberDstoreRemove).forEach(p -> p.snd.destroy());
        var portsRemoved = pairs.stream().limit(numberDstoreRemove).map(p -> p.fst).collect(Collectors.toList());

        var pairJoining = createDStoreProcessesWithFolders(numberDstoreJoining);

        Thread.sleep(300);

        controller.setCanRebalance(true);
        Thread.sleep(rebalancePeriod*files.size());

        var ports = Stream.concat(
                pairs.stream().skip(numberDstoreRemove).map(p -> p.fst),
                pairJoining.fst.stream()
                ).collect(Collectors.toList());


        for(var par : files.stream().filter(p -> controller.getFileAllocation().containsKey(p.fst)).collect(Collectors.toList())){

            int counter = 0;
            for(int port : ports){
                File file = new File(String.format("port_%s",port));
                if (Arrays.stream(file.list()).collect(Collectors.toList()).contains(par.fst)){
                    Assertions.assertTrue(controller.getFileAllocation().get(par.fst).contains(port));
                    FileInputStream input = new FileInputStream(String.format("port_%s",port) + File.separator + par.fst);
                    String actualContent = new String(input.readAllBytes());
                    input.close();
                    Assertions.assertTrue((actualContent.equals(par.snd+ "\r\n")) || par.snd.equals(actualContent));
                    counter++;
                }
            }
            Assertions.assertEquals(controller.getReplicationFactor(),counter);
        }


        for(var port : pairJoining.fst){
            Assertions.assertTrue(controller.getDstorePorts().contains(port));
        }


        for(var file : controller.getFileAllocation().keySet()){
            Assertions.assertEquals(controller.getReplicationFactor(),controller.getFileAllocation().get(file).size());
        }

        //Check whether the files are evenly distributed
        int numberFiles = controller.getFileAllocation().size();
        Assertions.assertEquals(numberDstoresStarting - numberDstoreRemove + numberDstoreJoining,controller.getDstorePorts().size());
        int dstoreNumber = controller.getDstorePorts().size();
        int min = (int) Math.floor(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        int max = (int) Math.ceil(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        for(int port : ports){
            File file = new File(String.format("port_%s",port));
            Assertions.assertTrue(file.list().length == min || file.list().length == max);
        }

        deleteFolders(pair.fst);
        deleteFolders(pairJoining.fst);
        deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
        destroyProcesses(pair.snd);
        destroyProcesses(pairJoining.snd);

    }

    Stream<Arguments> argumentsForTestRebalanceOperationDstoreFailingJoining(){
        return Stream.of(
                firstArgumentRebalanceDstoreFailingJoining(),
                secondArgumentRebalanceDstoreFailingJoining(),
                thirdArgumentRebalanceDstoreFailingJoining(),
                fourthArgumentRebalanceDstoreFailingJoining(),
                fifthArgumentRebalanceDstoreFailingJoining(),
                sixthArgumentRebalanceDstoreFailingJoining(),
                seventhArgumentRebalanceDstoreFailingJoining(),
                eighthArgumentRebalanceDstoreFailingJoining(),
                ninthArgumentRebalanceDstoreFailingJoining(),
                tenthArgumentRebalanceDstoreFailingJoining(),
                eleventhArgumentRebalanceDstoreFailingJoining(),
                twelthArgumentRebalanceDstoreFailingJoining()
        );
    }

    Arguments firstArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
        }};

        return Arguments.of(files,5,2,2,3,300);
    }

    Arguments secondArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
        }};

        return Arguments.of(files,5,2,1,3,300);
    }

    Arguments thirdArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
        }};

        return Arguments.of(files,6,5,1,2,300);
    }

    Arguments fourthArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE VERY AGAIN"));
            add(new Pair<>("hello6.txt","HEY HEY HEY THERE AGAIN"));
        }};

        return Arguments.of(files,10,9,1,2,300);
    }

    Arguments fifthArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE VERY AGAIN"));
            add(new Pair<>("hello6.txt","HEY HEY HEY THERE AGAIN"));
        }};

        return Arguments.of(files,10,2,5,6,300);
    }

    Arguments sixthArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE VERY AGAIN"));
            add(new Pair<>("hello6.txt","HEY HEY HEY THERE AGAIN"));
        }};

        return Arguments.of(files,6,3,7,2,300);
    }

    Arguments seventhArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE VERY AGAIN"));
            add(new Pair<>("hello6.txt","HEY HEY HEY THERE AGAIN"));
        }};

        return Arguments.of(files,9,6,6,7,300);
    }

    Arguments eighthArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE VERY AGAIN"));
            add(new Pair<>("hello6.txt","HEY HEY HEY THERE AGAIN"));
        }};

        return Arguments.of(files,2,2,2,2,300);
    }

    Arguments ninthArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE VERY AGAIN"));
            add(new Pair<>("hello6.txt","HEY HEY HEY THERE AGAIN"));
        }};

        return Arguments.of(files,6,6,4,3,300);
    }

    Arguments tenthArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 10; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,10,1,9,1,300);
    }

    Arguments eleventhArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 10; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,5,1,1,5,300);
    }

    Arguments twelthArgumentRebalanceDstoreFailingJoining(){
        List<Pair<String,String>> files = new ArrayList<>();
        for(int i = 1; i <= 10; i++){
            files.add(new Pair<>(String.format("hello%s.txt",i),String.format("HELLO%s",i)));
        }

        return Arguments.of(files,5,1,4,5,300);
    }


    /***********************************END OF TESTS FOR REBALANCE OPERATIONS - DSTORE FAILING AND JOINING **************/

    /***********************************BEGINNING OF TESTS REMOVE OPERATIONS********************************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestRemoveOperation")
    void testRemoveOperation(List<Pair<String,String>> files, String fileToRemove, int numberDstores, int replicationFactor) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,REBALANCE_PERIOD);

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        for(var par : files){
            client.store(createFile(par.fst,par.snd));
        }

        Thread.sleep(3000);

        client.remove(fileToRemove);

        Assertions.assertFalse(controller.getFileAllocation().containsKey(fileToRemove));
        Assertions.assertFalse(controller.getFileSizes().containsKey(fileToRemove));
        Assertions.assertFalse(controller.getRemovingOperation().get());

        deleteFolders(pair.fst);
        deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForTestRemoveOperation(){
        return Stream.of(
                firstArgumentRemoveOperation(),
                secondArgumentRemoveOperation(),
                thirdArgumentRemoveOperation(),
                fourthArgumentRemoveOperation(),
                fifthArgumentRemoveOperation()
        );
    }

    Arguments firstArgumentRemoveOperation(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE VERY AGAIN"));
            add(new Pair<>("hello6.txt","HEY HEY HEY THERE AGAIN"));
        }};

        return Arguments.of(files,"hello3.txt",6,3);
    }

    Arguments secondArgumentRemoveOperation(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
            add(new Pair<>("hello5.txt","HEY THERE VERY AGAIN"));
            add(new Pair<>("hello6.txt","HEY HEY HEY THERE AGAIN"));
        }};

        return Arguments.of(files,"hello.txt",5,2);
    }

    Arguments thirdArgumentRemoveOperation(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
        }};

        return Arguments.of(files,"hello1.txt",10,10);
    }

    Arguments fourthArgumentRemoveOperation(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
        }};

        return Arguments.of(files,"hello1.txt",5,4);
    }

    Arguments fifthArgumentRemoveOperation(){
        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","HEY THERE"));
            add(new Pair<>("hello1.txt","HEY THERE AGAIN"));
            add(new Pair<>("hello2.txt","HEY THERE THERE AGAIN"));
            add(new Pair<>("hello3.txt","HEY HEY THERE AGAIN"));
            add(new Pair<>("hello4.txt","HEY BEY THERE AGAIN"));
        }};

        return Arguments.of(files,"hello3.txt",3,2);
    }


    /***********************************END OF TESTS REMOVE OPERATIONS********************************************/

    /***********************************BEGINNING OF TESTS REMOVE OPERATIONS**************************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestRemoveTimeoutOperation")
    void testRemoveTimeoutOperation(int numberDstores, int timeout, int replicationFactor) throws InterruptedException, IOException {

        resetController(replicationFactor,timeout,REBALANCE_PERIOD);

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);
        controller.getFileAllocation().put("hello.txt",pair.fst);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        try {
            client.remove("hello.txt");
        }catch (Exception e){

        }
        Thread.sleep(timeout*10);

        Assertions.assertFalse(controller.getAcknowledges().containsKey("hello.txt"));

        deleteFolders(pair.fst);
        deleteFiles(new ArrayList<>(){{add("hello.txt");}});
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForTestRemoveTimeoutOperation(){
        return Stream.of(
                Arguments.of(10,15,10)
        );
    }

    /**************************************END OF TESTS REMOVE OPERATIONS*****************************************/

    /***********************************BEGINNING OF TESTS REMOVE OPERATIONS FAILURES*****************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestRemoveOperationFailure")
    void testRemoveOperationFailure(List<Pair<String,String>> files, String fileToRemove, int numberDstores, int replicationFactor) throws InterruptedException, IOException {

        resetController(replicationFactor,TIME_OUT,REBALANCE_PERIOD);

        var pair = createDStoreProcessesWithFolders(numberDstores);

        Thread.sleep(3000);

        Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
        client.connect();

        if(numberDstores < replicationFactor){
            Assertions.assertThrows(IOException.class,()->client.remove(fileToRemove));
            deleteFolders(pair.fst);
            deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
            destroyProcesses(pair.snd);
        }else {

            for(var par : files){
                client.store(createFile(par.fst,par.snd));
            }

            Thread.sleep(3000);

            try {
                client.remove(fileToRemove);
                deleteFolders(pair.fst);
                deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
                destroyProcesses(pair.snd);
                Assertions.assertTrue(false);

            } catch (NotEnoughDstoresException | FileDoesNotExistException e) {
                deleteFolders(pair.fst);
                deleteFiles(files.stream().map(p -> p.fst).collect(Collectors.toList()));
                destroyProcesses(pair.snd);
                Assertions.assertTrue(true);
            }
        }
    }

    Stream<Arguments> argumentsForTestRemoveOperationFailure() {
        return Stream.of(
                firstArgumentRemoveOperationFailure(),
                secondArgumentRemoveOperationFailure(),
                thirdArgumentRemoveOperationFailure(),
                fourthArgumentRemoveOperationFailure()
        );
    }

    Arguments firstArgumentRemoveOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,"hello.txt",2,4);
    }

    Arguments secondArgumentRemoveOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO"));
            add(new Pair<>("hello2.txt","CIAO SONO IO"));
            add(new Pair<>("hello3.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,"hello.txt",6,10);
    }

    Arguments thirdArgumentRemoveOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO"));
            add(new Pair<>("hello2.txt","CIAO SONO IO"));
            add(new Pair<>("hello3.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,"hell.txt",6,3);
    }

    Arguments fourthArgumentRemoveOperationFailure(){

        List<Pair<String,String>> files = new ArrayList<>(){{
            add(new Pair<>("hello.txt","CIAO SONO IO"));
            add(new Pair<>("hello1.txt","CIAO SONO IO"));
            add(new Pair<>("hello2.txt","CIAO SONO IO"));
            add(new Pair<>("hello3.txt","CIAO SONO IO"));
        }};

        return Arguments.of(files,"ben.txt",5,5);
    }

    /***********************************END OF TESTS REMOVE OPERATIONS FAILURES*****************************/

    /***********************************BEGINNING OF TESTS SIMULATIONS**************************************/

    @ParameterizedTest
    @MethodSource("argumentsForTestSimulations")
    void testSimulations(List<List<Pair<String,String>>> filesToStore, List<List<String>> filesToRemove, List<String> filesStoredAfter, Pair<String,String> fileContentToLoad, int numberDstoresStarting, int replicationFactor,int rebalancePeriod) throws InterruptedException, IOException {

        CountDownLatch countDownLatchStoreRemove = new CountDownLatch(filesToStore.size());
        CountDownLatch countDownLatchRemoveListLoad = new CountDownLatch(filesToRemove.size());
        CountDownLatch countDownLatchEnd = new CountDownLatch(2);

        resetController(replicationFactor,TIME_OUT,rebalancePeriod);
        controller.setCanRebalance(true);

        var pair = createDStoreProcessesWithFolders(numberDstoresStarting);

        Thread.sleep(1000);

        //STORE FILES
        List<Callable<Void>> tasksStore = new ArrayList<>();
        for(var list : filesToStore){

            tasksStore.add(()->{
                Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
                try {
                    client.connect();
                    for(var par : list){
                        client.store(createFile(par.fst,par.snd));
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
                countDownLatchStoreRemove.countDown();
                return null;
            });
        }
        Executors.newFixedThreadPool(10).invokeAll(tasksStore);

        countDownLatchStoreRemove.await();

        //Remove files
        List<Callable<Void>> tasksRemove = new ArrayList<>();
        for(var list : filesToRemove){

            tasksRemove.add(()->{
                Client client = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
                try {
                    client.connect();
                    for(var fileName : list){
                        client.remove(fileName);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                countDownLatchRemoveListLoad.countDown();
                return null;
            });
        }
        Executors.newFixedThreadPool(10).invokeAll(tasksRemove);

        countDownLatchRemoveListLoad.await();

        List<Callable<Void>> listLoad = new ArrayList<>();
        listLoad.add(()->{
            Client clientList = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
            clientList.connect();
            Collections.sort(filesStoredAfter);
            var l = Arrays.stream(clientList.list()).collect(Collectors.toList());
            Collections.sort(l);
            Assertions.assertLinesMatch(filesStoredAfter,l);
            return null;
        });
        listLoad.add(()->{
            Client clientLoad = new Client(controllerPort,controller.getTimeout(), Logger.LoggingType.ON_FILE_AND_TERMINAL);
            clientLoad.connect();
            String actualContent = new String(clientLoad.load(fileContentToLoad.fst));
            Assertions.assertTrue(actualContent.equals(fileContentToLoad.snd + "\r\n") || actualContent.equals(fileContentToLoad.snd));
            return null;
        });

        boolean bool = Executors.newFixedThreadPool(2).invokeAll(listLoad).stream()
                .map(r-> {
                    try {
                        r.get();
                        countDownLatchEnd.countDown();
                        return true;
                    } catch (InterruptedException | ExecutionException e) {
                        countDownLatchEnd.countDown();
                        System.out.println(false);
                        return false;
                    }
                }).filter(b -> !b).count() == 0;

        countDownLatchEnd.await();
        Assertions.assertTrue(bool);

        Thread.sleep(3000);

        //Check whether the files are evenly distributed
        int numberFiles = controller.getFileAllocation().size();
        Assertions.assertEquals(numberDstoresStarting,controller.getDstorePorts().size());
        int dstoreNumber = controller.getDstorePorts().size();
        int min = (int) Math.floor(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        int max = (int) Math.ceil(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
        for(int port : pair.fst){
            File file = new File(String.format("port_%s",port));
            Assertions.assertTrue(file.list().length == min || file.list().length == max);
        }

        deleteFolders(pair.fst);
        filesToStore.stream().map(l -> l.stream().map(p -> p.fst).collect(Collectors.toList())).forEach(f -> deleteFiles(f));
        destroyProcesses(pair.snd);

    }

    Stream<Arguments> argumentsForTestSimulations(){
        return Stream.of(
                firstArgumentSimulation(),
                secondArgumentSimulation(),
                thirdArgumentSimulation(),
                fourthArgumentSimulation(),
                fifthArgumentSimulation(),
                sixthArgumentSimulation()
        );
    }

    Arguments firstArgumentSimulation(){
        List<List<Pair<String,String>>> filesToStore = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add(new Pair<>("hello.txt","HEY HEY"));
                add(new Pair<>("hello1.txt","HEY HEY HELLO"));
            }});
        }};

        List<List<String>> filesToRemove = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add("hello.txt");
            }});
        }};

        List<String> filesStoredAfter = new ArrayList<>(){{
            add("hello1.txt");
        }};

        Pair<String,String> fileContentToLoad = new Pair<>("hello1.txt","HEY HEY HELLO");

        return  Arguments.of(filesToStore,filesToRemove,filesStoredAfter,fileContentToLoad,3,2,300);
    }

    Arguments secondArgumentSimulation(){

        List<List<Pair<String,String>>> filesToStore = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add(new Pair<>("hello.txt","HEY HEY"));
                add(new Pair<>("hello1.txt","HEY HEY HELLO"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello2.txt","HEY HEY"));
                add(new Pair<>("hello3.txt","HEY HEY HELLO"));
            }});

        }};

        List<List<String>> filesToRemove = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add("hello.txt");
            }});
            add(new ArrayList<>(){{
                add("hello2.txt");
            }});

        }};

        List<String> filesStoredAfter = new ArrayList<>(){{
            add("hello1.txt");
            add("hello3.txt");
        }};


        Pair<String,String> fileContentToLoad = new Pair<>("hello3.txt","HEY HEY HELLO");

        return  Arguments.of(filesToStore,filesToRemove,filesStoredAfter,fileContentToLoad,2,1,300);
    }

    Arguments thirdArgumentSimulation(){

        List<List<Pair<String,String>>> filesToStore = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add(new Pair<>("hello.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello1.txt","HEY HEY HELLO"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello2.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello3.txt","HEY HEY HELLO"));
            }});

        }};

        List<List<String>> filesToRemove = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add("hello.txt");
            }});
            add(new ArrayList<>(){{
                add("hello2.txt");
            }});

        }};

        List<String> filesStoredAfter = new ArrayList<>(){{
            add("hello1.txt");
            add("hello3.txt");
        }};


        Pair<String,String> fileContentToLoad = new Pair<>("hello3.txt","HEY HEY HELLO");

        return  Arguments.of(filesToStore,filesToRemove,filesStoredAfter,fileContentToLoad,2,1,300);
    }

    Arguments fourthArgumentSimulation(){

        List<List<Pair<String,String>>> filesToStore = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add(new Pair<>("hello.txt","HEY HEY"));
                add(new Pair<>("hello4.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello1.txt","HEY HEY HELLO"));
                add(new Pair<>("hello5.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello2.txt","HEY HEY"));
                add(new Pair<>("hello6.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello3.txt","HEY HEY HELLO"));
                add(new Pair<>("hello7.txt","HEY HEY"));
            }});

        }};

        List<List<String>> filesToRemove = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add("hello.txt");
            }});
            add(new ArrayList<>(){{
                add("hello2.txt");
            }});
            add(new ArrayList<>(){{
                add("hello1.txt");
                add("hello3.txt");
            }});


        }};

        List<String> filesStoredAfter = new ArrayList<>(){{
            add("hello4.txt");
            add("hello5.txt");
            add("hello6.txt");
            add("hello7.txt");
        }};


        Pair<String,String> fileContentToLoad = new Pair<>("hello5.txt","HEY HEY");

        return  Arguments.of(filesToStore,filesToRemove,filesStoredAfter,fileContentToLoad,4,3,300);
    }

    Arguments fifthArgumentSimulation(){

        List<List<Pair<String,String>>> filesToStore = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add(new Pair<>("hello.txt","HEY HEY"));
                add(new Pair<>("hello4.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello1.txt","HEY HEY HELLO"));
                add(new Pair<>("hello5.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello2.txt","HEY HEY"));
                add(new Pair<>("hello6.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello3.txt","HEY HEY HELLO"));
                add(new Pair<>("hello7.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello8.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello9.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello10.txt","HEY HEY"));
            }});

        }};

        List<List<String>> filesToRemove = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add("hello.txt");
            }});
            add(new ArrayList<>(){{
                add("hello2.txt");
            }});
            add(new ArrayList<>(){{
                add("hello1.txt");
                add("hello3.txt");
            }});
            add(new ArrayList<>(){{
                add("hello8.txt");
            }});
            add(new ArrayList<>(){{
                add("hello9.txt");
            }});
            add(new ArrayList<>(){{
                add("hello10.txt");
            }});


        }};

        List<String> filesStoredAfter = new ArrayList<>(){{
            add("hello4.txt");
            add("hello5.txt");
            add("hello6.txt");
            add("hello7.txt");
        }};


        Pair<String,String> fileContentToLoad = new Pair<>("hello5.txt","HEY HEY");

        return  Arguments.of(filesToStore,filesToRemove,filesStoredAfter,fileContentToLoad,8,3,300);
    }

    Arguments sixthArgumentSimulation(){

        List<List<Pair<String,String>>> filesToStore = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add(new Pair<>("hello.txt","HEY HEY"));
                add(new Pair<>("hello4.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello1.txt","HEY HEY HELLO"));
                add(new Pair<>("hello5.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello2.txt","HEY HEY"));
                add(new Pair<>("hello6.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello3.txt","HEY HEY HELLO"));
                add(new Pair<>("hello7.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello8.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello9.txt","HEY HEY"));
            }});
            add(new ArrayList<>(){{
                add(new Pair<>("hello10.txt","HEY HEY"));
            }});

        }};

        List<List<String>> filesToRemove = new ArrayList<>(){{
            add(new ArrayList<>(){{
                add("hello.txt");
            }});
            add(new ArrayList<>(){{
                add("hello2.txt");
            }});
            add(new ArrayList<>(){{
                add("hello1.txt");
            }});
            add(new ArrayList<>(){{
                add("hello3.txt");
            }});
        }};

        List<String> filesStoredAfter = new ArrayList<>(){{
            add("hello4.txt");
            add("hello5.txt");
            add("hello6.txt");
            add("hello7.txt");
            add("hello8.txt");
            add("hello9.txt");
            add("hello10.txt");
        }};


        Pair<String,String> fileContentToLoad = new Pair<>("hello8.txt","HEY HEY");

        return  Arguments.of(filesToStore,filesToRemove,filesStoredAfter,fileContentToLoad,10,6,300);
    }

    /**************************************END OF TESTS SIMULATIONS*****************************************/

    /**************************************BEGINNING OF LISTING TIMEOUTS TESTS******************************/

    @ParameterizedTest
    @MethodSource("argumentsForListTimeoutOperation")
    void testListTimeoutsOperation(int numberDstores, int timeout) throws InterruptedException, IOException {

        resetController(REPLICATION_FACTOR,timeout,300);
        controller.getFileAllocation().put("hello.txt",new ArrayList<>(){{add(1024);add(1025);}});
        controller.getFileAllocation().put("hello1.txt",new ArrayList<>(){{add(1024);add(1025);}});
        var pair = createDStoreProcessesWithFolders(numberDstores);
        Thread.sleep(3000);
        controller.setCanRebalance(true);
        Thread.sleep(3000);
        System.out.println("Now the dstores are: ");
        for(int port : controller.getDstorePorts()){
            System.out.println(port);
        }
//        for(var par : files){
//            int counter = 0;
//            for(int port : pair.fst){
//                File file = new File(String.format("port_%s",port));
//                if (Arrays.stream(file.list()).collect(Collectors.toList()).contains(par.fst)){
//                    Assertions.assertTrue(controller.getFileAllocation().get(par.fst).contains(port));
//                    FileInputStream input = new FileInputStream(String.format("port_%s",port) + File.separator + par.fst);
//                    String actualContent = new String(input.readAllBytes());
//                    input.close();
//                    Assertions.assertTrue((actualContent.equals(par.snd+ "\r\n")) || par.snd.equals(actualContent));
//                    counter++;
//                }
//            }
//            Assertions.assertEquals(controller.getReplicationFactor(),counter);
//        }
//
//        //Check whether the files are evenly distributed
//        int numberFiles = controller.getFileAllocation().size();
//        int dstoreNumber = controller.getDstorePorts().size();
//        int min = (int) Math.floor(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
//        int max = (int) Math.ceil(numberFiles*controller.getReplicationFactor()/Double.valueOf(dstoreNumber));
//        for(int port : pair.fst){
//            File file = new File(String.format("port_%s",port));
//            Assertions.assertTrue(file.list().length == min || file.list().length == max);
//        }

        deleteFolders(pair.fst);
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForListTimeoutOperation(){
        return Stream.of(
                Arguments.of(10,0),
                Arguments.of(20,15),
                Arguments.of(20,10)
        );
    }

    /*******************************************BEGINNING OF DELETE DIR CONTENT DSTORE*******************************/

    @ParameterizedTest
    @MethodSource("argumentsForDeleteDirContent")
    void testDeleteDirContent(String folder) throws InterruptedException, IOException {

        var pair = createDStoreProcesses(1,folder);
        Thread.sleep(1000);

        Assertions.assertEquals(0,new File(folder).list().length);

        deleteDir(new File(folder));
        destroyProcesses(pair.snd);
    }

    Stream<Arguments> argumentsForDeleteDirContent() throws IOException {
        return Stream.of(
                firstArgumentDeleteDir(),
                secondArgumentDeleteDir()
        );
    }

    Arguments firstArgumentDeleteDir() throws IOException {
        File file = new File("hello");
        file.mkdir();
        File file1 = new File(file.getAbsolutePath() + File.separator + "hello.txt");
        file1.createNewFile();
        return Arguments.of("hello");
    }

    Arguments secondArgumentDeleteDir() throws IOException {
        File file = new File("hello");
        file.mkdir();
        File file1 = new File(file.getAbsolutePath() + File.separator + "hello.txt");
        file1.createNewFile();
        File file2 = new File(file.getAbsolutePath() + File.separator + "hello1");
        file2.mkdir();
        File file3 = new File(file2.getAbsolutePath() + File.separator + "hello1.txt");
        file3.createNewFile();
        return Arguments.of("hello");
    }


    /*********************************************END OF DELETE DIR CONTENT DSTORE**********************************/


    /*********************************************BEGINNING OF REBALANCE DSTORE JOINING*****************************/


    /************************************************END OF REBALANCE DSTORE JOINING********************************/


    /*****************************************END OF LISTING TIMEOUTS TESTS********************************/


    /*********************************BEGINNING OF DSTORE CREATION UTILITIES**************************/

    Pair<List<Dstore>,List<Thread>> createDstoreThreads(List<Pair<Integer,List<String>>> dstoresFiles) throws InterruptedException, IOException {

        List<Dstore> dstores = new ArrayList<>();
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for(var pair : dstoresFiles){
            Dstore dstore = new Dstore(pair.fst,controllerPort,controller.getTimeout(),"");
            setUpDstore(dstore,pair.snd);
            dstores.add(dstore);
            tasks.add(dstore::joinController);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Boolean> results = executorService.invokeAll(tasks).stream()
                .map(r -> {
                    try {
                        return r.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .collect(Collectors.toList());

        for(boolean b : results){
            System.out.println(b);
        }

        List<Thread> threads = new ArrayList<>();
        for(Dstore dstore : dstores){
            Thread thread = new Thread(dstore::waitForRequestsFromClients);
            threads.add(thread);
            thread.start();

        }


        Thread.sleep(300);
        return new Pair<>(dstores,threads);

    }

    void interruptThreads(List<Thread> threads){
        threads.stream().forEach(t -> t.stop());
    }

    void setUpDstore(Dstore dstore,List<String> files) throws IOException {

        String fileFolder = String.format("port_%s",dstore.getPort());
        dstore.setFileFolder(fileFolder);
        File file = new File(fileFolder);
        file.mkdir();
        for(String fil : files){
            dstore.getFileSizes().put(fil, 34L);
            file = new File(fileFolder + File.separator + fil);
            file.createNewFile();
            FileInputStream fileInputStream = new FileInputStream(new File("Notes.txt"));
            byte[] content = fileInputStream.readAllBytes();
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            fileOutputStream.write(content);
            fileInputStream.close();
            fileOutputStream.close();
        }
    }

    void deleteDstoreFolders(List<Dstore> dstores){
        for(var dstore : dstores){
            deleteDir(new File(dstore.getFileFolder()));
        }
    }

    void deleteFolders(List<Integer> dstores){
        for(int port : dstores){
            deleteDir(new File(String.format("port_%s",port)));
        }
    }

    void deleteFiles(List<String> files){
        for(var file : files){
            new File(file).delete();
        }
    }

    void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if(files != null) {
            for (File file : files) {
                deleteDir(file);
            }
        }
        dir.delete();
    }

    void deleteDirContent(File dir){
        File[] files = dir.listFiles();
        for(var file : files){
            deleteDir(file);
        }
    }

    List<Integer> takeRandomPorts(List<Integer> freePorts, int n){
        List<Integer> rands = new ArrayList<>();
        Random random = new Random();
        for(int i = 0; i < n; i++){
            rands.add(freePorts.get(random.nextInt(freePorts.size())));
        }
        return rands;
    }

    private List<Integer> listFreePorts(){
        List<Integer> freePorts = new ArrayList<>();
        for(int i = 1023; i <= 10000; i++){
            try(ServerSocket socket = new ServerSocket(i)){
                freePorts.add(i);
            } catch (IOException e) {
                //DO nothing occupied port
            }
        }
        return freePorts;
    }

    Pair<List<Integer>,List<Process>> createDStoreProcesses(int n) throws IOException {
        List<Process> processes = new ArrayList<>();
        List<Integer> freePorts = listFreePorts().stream().limit(n).collect(Collectors.toList());
        for(int i = 0; i < n; i++){
            Process process = Runtime.getRuntime().exec(String.format("java Dstore %s %s %s hello",freePorts.get(i),controllerPort,controller.getTimeout()));
            processes.add(process);
        }
        return new Pair<>(freePorts,processes);
    }

    Pair<List<Integer>,List<Process>> createDStoreProcesses(int n,String folder) throws IOException {
        List<Process> processes = new ArrayList<>();
        List<Integer> freePorts = listFreePorts().stream().limit(n).collect(Collectors.toList());
        for(int i = 0; i < n; i++){
            Process process = Runtime.getRuntime().exec(String.format("java Dstore %s %s %s %s",freePorts.get(i),controllerPort,controller.getTimeout(),folder));
            processes.add(process);
        }
        return new Pair<>(freePorts,processes);
    }

    Pair<List<Integer>,List<Process>> createDStoreProcessesWithFolders(int n) throws IOException {
        List<Process> processes = new ArrayList<>();
        List<Integer> freePorts = listFreePorts().stream().limit(n).collect(Collectors.toList());
        for(int i = 0; i < n; i++){
            String fileFolder = String.format("port_%s",freePorts.get(i));
            File file = new File(fileFolder);
            file.mkdir();
            Process process = Runtime.getRuntime().exec(String.format("java Dstore %s %s %s %s",freePorts.get(i),controllerPort,controller.getTimeout(),fileFolder));
            processes.add(process);
        }
        return new Pair<>(freePorts,processes);
    }

    void destroyProcesses(List<Process> processes){
        processes.stream().forEach(p -> {
            try {
                p.destroy();
            } catch (Exception e) {
                System.out.println("Destroy process exception");
            }

        });
    }

    /*********************************END OF DSTORE CREATION UTILITIES**************************************/


}
