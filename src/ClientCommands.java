public enum ClientCommands {
    STORE_TO,
    ACK,
    STORE_COMPLETE,
    ERROR_FILE_ALREADY_EXISTS,
    LOAD_FROM,
    ERROR_FILE_DOES_NOT_EXIST,
    ERROR_LOAD,
    REMOVE_COMPLETE,
    ERROR_NOT_ENOUGH_DSTORES,
    LIST,
    NoCommand;




    public static void main(String[] args) {
        System.out.println(ClientCommands.STORE_TO.toString());
    }
}
