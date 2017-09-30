/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Ashwin
 */
public class Main{

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    private static String CONFIG_FILE="src/dsConfig";
    private static BufferedReader BR;
    private static String LINE;
    private static String HOST = "";
    private static int PORT = 1712;
    private static int MAX_PROC_COUNT;
    private static final Map<Integer, Integer[]> NEIGHBOUR_LIST = new HashMap<>();
    public static int INTERVAL;
    public static int TERMINATE;
    
    public static void main(String[] args) throws IOException {
        
        BR = new BufferedReader(new FileReader(CONFIG_FILE));
        String nb_line;
        String[] processList;
        String option;
        Integer[] processNeighbours;
        while((LINE = BR.readLine()) != null) {
            if (LINE.startsWith("COORDINATOR")) {
                HOST = LINE.split("COORDINATOR")[1].trim();
            }
            
            if(LINE.startsWith("NUMBER")){
                MAX_PROC_COUNT = Integer.parseInt(LINE.split("NUMBER OF PROCESSES")[1].trim());
            }
            
            if(LINE.startsWith("INTERVAL")) {
                INTERVAL = Integer.parseInt(LINE.split("INTERVAL")[1].trim());
            }
            if(LINE.startsWith("TERMINATE")) {
                TERMINATE = Integer.parseInt(LINE.split("TERMINATE")[1].trim());
            }
            if (LINE.startsWith("NEIGHBOUR")) {
                while((nb_line = BR.readLine())!=null) {
                    processList = nb_line.split(" ");
                    processNeighbours = new Integer[processList.length];
                    for(int i=1;i<processList.length;i++) {
                        processNeighbours[i] = Integer.parseInt(processList[i]);
                    }
                    NEIGHBOUR_LIST.put(Integer.parseInt(processList[0]),processNeighbours);
                    processNeighbours = null;
                }
            }
        }
        if(args.length > 0) {
            option = args[0];
        } else {
            option = "";
        }
        
        switch (option) {
            case "-c":
                new COORDINATOR(PORT, 
                        CONFIG_FILE, 
                        NEIGHBOUR_LIST, 
                        MAX_PROC_COUNT).start();
                break;
            default:
                new PROCESS(HOST, PORT, NEIGHBOUR_LIST, MAX_PROC_COUNT).start();
        }

    }
    
}


class COORDINATOR {

    private static ServerSocket coordinatorSocket = null;
    private static Socket processSocket = null;
    private final int MAX_PROC_COUNT;
    private static int PROC_INDEX = 1;
    private final String CONFIG_FILE;
    public static int L_CLOCK = 0;
    public static Integer[] SENT;
    public static Integer[] RECV;
    public static Integer[] CHANNEL;
    public static int READY_COUNT = 0;
    private final Map<Integer, Integer[]> NEIGHBOUR_LIST;
    private static int INTERVAL;
    private static int TERMINATE;
    private Map<Socket, Integer> PROCESS_IDS = new HashMap<>();
    
    private int PORT;
    
    /*
     * Initialize the COORDINATOR constructor with all the passed
     * parameters. 
     */
    public COORDINATOR(int PORT, 
            String CONFIG_FILE, 
            Map<Integer, Integer[]> NEIGHBOUR_LIST, 
            int MAX_PROC_COUNT
            ) {
        
        this.PORT = PORT;
        this.CONFIG_FILE = CONFIG_FILE;
        this.MAX_PROC_COUNT = MAX_PROC_COUNT;
        this.NEIGHBOUR_LIST = NEIGHBOUR_LIST;
        this.SENT = new Integer[this.MAX_PROC_COUNT+1];
        this.RECV = new Integer[this.MAX_PROC_COUNT+1];
        this.CHANNEL = new Integer[this.MAX_PROC_COUNT+1];
        Arrays.fill(SENT, 0);
        Arrays.fill(RECV, 0);
        Arrays.fill(CHANNEL, 0);

    }
    

    public void start() {
    
        try {
            coordinatorSocket = new ServerSocket(PORT);
            coordinatorSocket.setReuseAddress(true);
            coordinatorSocket.setSoTimeout(1000*60*60);
            PROC_INDEX++;
        } catch (IOException ex) {
            Logger.getLogger(COORDINATOR.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println("COORDINATOR started at <"+coordinatorSocket.getLocalSocketAddress()+">.");

        //Continously accept new connections form individual clients.
        //Pass every accepted socket into a handler thread.
        //Every passed socket then can communicate separately with the Server (Coordinator).
        try {
            
            while(PROC_INDEX<=MAX_PROC_COUNT) {
                processSocket = coordinatorSocket.accept();
                PROCESS_IDS.put(processSocket,PROC_INDEX);
                new processThreadHandler(processSocket,PROCESS_IDS,MAX_PROC_COUNT, NEIGHBOUR_LIST).start();
                if (PROC_INDEX == MAX_PROC_COUNT) {
                    System.out.println("MAX PROCESS COUNT REACHED.");
                    break;
                }
                PROC_INDEX++;
            }
        } catch (IOException ex) {
            Logger.getLogger(COORDINATOR.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}


class processThreadHandler extends Thread {
    
    private final Socket processSocket;
    private BufferedReader inStream;
    private PrintWriter outStream;
    private final Map<Socket, Integer> PROCESS_IDS;
    private Map<Integer, Integer[]> NEIGHBOUR_LIST;
    private final int MAX_PROC_COUNT;
    private int[] MC_VAL = new int[]{0,1,2};
    private int RECORD = 0;
    BufferedWriter fileWriter;
    public processThreadHandler(Socket processSocket, Map<Socket, Integer> PROCESS_IDS, int MAX_PROC_COUNT, Map<Integer, Integer[]> NEIGHBOUR_LIST) {
        this.processSocket = processSocket;
        this.PROCESS_IDS = PROCESS_IDS;
        this.MAX_PROC_COUNT = MAX_PROC_COUNT;
        this.NEIGHBOUR_LIST = NEIGHBOUR_LIST;
        try {
            fileWriter = new BufferedWriter(new FileWriter("localstate_1"));
        } catch (IOException ex) {
            Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void save_state(){
        try {
            fileWriter.write("SENT "+Arrays.toString(Arrays.copyOfRange(COORDINATOR.SENT, 1, COORDINATOR.SENT.length))+"\n");
            fileWriter.write("RECV "+Arrays.toString(Arrays.copyOfRange(COORDINATOR.RECV, 1, COORDINATOR.RECV.length))+"\n");
            fileWriter.write("CHANNEL "+Arrays.toString(Arrays.copyOfRange(COORDINATOR.CHANNEL, 1, COORDINATOR.CHANNEL.length))+"\n");
            fileWriter.write("RECORDING COUNT = "+RECORD+"\n");
            fileWriter.close();
        } catch (IOException ex) {
            Logger.getLogger(PROCESS.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    private Object getKeyFromValue(Map map, Integer value){
        for(Object o: map.keySet()){
            if(map.get(o).equals(value)){
                return o;
            }
        }
        return null;
    }
    
    private void sendMsgToNB(String msg, int neighbourPID){
        try {
            PrintWriter os;
            msg += ",TS="+COORDINATOR.L_CLOCK;
            Socket nb = (Socket)getKeyFromValue(PROCESS_IDS, neighbourPID);
            os = new PrintWriter(nb.getOutputStream(), true);
            os.println(msg);
            System.out.println("SENT=@<"+nb.getRemoteSocketAddress()+">$:"+msg);
            COORDINATOR.L_CLOCK++;
            COORDINATOR.SENT[neighbourPID]++;
        } catch (IOException ex) {
            Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    private void sendMsgToCNBList(String msg, int clock) {
        //Get neighbours for process id =1,
        //i.e, the coordinator itself.
        PrintWriter os;
        String sendMsg = msg+",TS="+clock;
        for(Integer pid: NEIGHBOUR_LIST.get(1)){
            if(pid!=null){
                try {
                    Socket nb = (Socket)getKeyFromValue(PROCESS_IDS, pid);
                    os = new PrintWriter(nb.getOutputStream(), true);
                    os.println(sendMsg);
                    System.out.println("SENT=@COORDINATOR$:"+sendMsg);
                    clock++;
                    COORDINATOR.SENT[pid]++;
                } catch (IOException ex) {
                    Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
                }
                
            }
        }
    }
    
    private void randomSend(String msg){
        int rnd_val = new Random().nextInt(MC_VAL.length);
        int rnd_pindex;
        switch (MC_VAL[rnd_val]) {
            case 0:
                System.out.println("No message sent.");
                //break;
            case 1:
                System.out.println("Sending 1 message.");
                rnd_pindex = new Random().nextInt(NEIGHBOUR_LIST.get(1).length-1)+1;
                sendMsgToNB(msg, NEIGHBOUR_LIST.get(1)[rnd_pindex]);
                break;
            case 2:
                System.out.println("Sending 2 messages.");
                for(int i=1;i<=2;i++){
                    rnd_pindex = new Random().nextInt(NEIGHBOUR_LIST.get(1).length-1)+1;
                    sendMsgToNB(msg, NEIGHBOUR_LIST.get(1)[rnd_pindex]);
                }   break;
            default:
                break;
        }
    }
    
    private void broadcastNBList(Socket process) {
        PrintWriter os;
        String msg;
        String PID= "PID=";
        String NB_LIST="NB=";
        //Getting each process's neighbour names (host,port).
        for(Integer i: NEIGHBOUR_LIST.get(PROCESS_IDS.get(process))) {
            if(i!=null){
                Socket s;
                s = (Socket)getKeyFromValue(PROCESS_IDS, i);
                if (s!=null){
                    NB_LIST += "("+s.getInetAddress().getCanonicalHostName()+","+PROCESS_IDS.get(s).toString()+")" +"/";
                } else if (s ==null){
                    //If the neighbour is Coordinator, then no need to send (host,port).
                    NB_LIST += "COORDINATOR"+"/";
                }
            }
        }
        //Getting each process's pid
        PID += PROCESS_IDS.get(process).toString();
        msg = PID+";"+NB_LIST+";"+"TS="+COORDINATOR.L_CLOCK;
        try {
            os = new PrintWriter(process.getOutputStream(), true);
            os.println(msg);
            COORDINATOR.L_CLOCK++;
            COORDINATOR.SENT[PROCESS_IDS.get(process)]++;
            COORDINATOR.CHANNEL[PROCESS_IDS.get(process)]++;
            System.out.println("SENT=@COORDINATOR$:"+msg);

        } catch (IOException ex) {
            Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void run() {
        
        try {

            inStream = new BufferedReader(new InputStreamReader(processSocket.getInputStream()));
            outStream = new PrintWriter(processSocket.getOutputStream(), true);
            System.out.println("@<"+processSocket.getRemoteSocketAddress()+"> has joined.");
            
            String sendMsg, recvMsg;
            while((recvMsg = inStream.readLine())!=null) {
                //System.out.println("my clock="+COORDINATOR.L_CLOCK);
                if( (COORDINATOR.L_CLOCK % Main.INTERVAL == 0) && COORDINATOR.L_CLOCK>0){
                    /**
                     * Marker Sending Rule.
                     **/
                    if(RECORD == 0){
                        /**
                         * Initial marker.
                         **/
                        save_state();
                        new MarkerSend(outStream, processSocket, PROCESS_IDS.get(processSocket), false, true).start();
                        RECORD++;
                    }
                    
                }
                
                if(COORDINATOR.L_CLOCK == Main.TERMINATE){
                    save_state();
                    new MarkerSend(outStream, processSocket, PROCESS_IDS.get(processSocket), true, true).start();
                }
                if(recvMsg.startsWith("REGISTER")){
                    System.out.println("RECEIVED=@<"+processSocket.getRemoteSocketAddress()+">$:"+recvMsg);
                    int TS = Integer.parseInt(recvMsg.split(",TS=")[1]);
                    COORDINATOR.L_CLOCK = Integer.max(COORDINATOR.L_CLOCK, TS+1);
                    COORDINATOR.RECV[PROCESS_IDS.get(processSocket)]++;
                    if((MAX_PROC_COUNT-1)==PROCESS_IDS.size()){
                        
                        for(Socket process: PROCESS_IDS.keySet()){
                            broadcastNBList(process);
                        }
                    }
                } 
                if(recvMsg.startsWith("READY")){
                    COORDINATOR.READY_COUNT++;
                    int TS = Integer.parseInt(recvMsg.split(",TS=")[1]);
                    COORDINATOR.L_CLOCK = Integer.max(COORDINATOR.L_CLOCK, TS+1);
                    COORDINATOR.RECV[PROCESS_IDS.get(processSocket)]++;
                    COORDINATOR.CHANNEL[PROCESS_IDS.get(processSocket)]--;
                    System.out.println("RECEIVED=@<"+processSocket.getRemoteSocketAddress()+">$:"+recvMsg);
                    sendMsg = "COMPUTE";
                    if(COORDINATOR.READY_COUNT == (MAX_PROC_COUNT-1)){
                        sendMsgToCNBList(sendMsg, COORDINATOR.L_CLOCK);
                    }
                }
                
                if(recvMsg.startsWith("COMPUTE")){
                    int TS = Integer.parseInt(recvMsg.split(",TS=")[1]);
                    COORDINATOR.L_CLOCK = Integer.max(COORDINATOR.L_CLOCK, TS+1);
                    COORDINATOR.RECV[PROCESS_IDS.get(processSocket)]++;
                    System.out.println("RECEIVED=@<"+processSocket.getRemoteSocketAddress()+">$:"+recvMsg);
                    int random_sleep = new Random().nextInt(5-1)+1;
                    randomSend("COMPUTE");
                }
                /*if(COORDINATOR.L_CLOCK == Main.TERMINATE){
                    System.exit(0);
                }*/
            }
            //inStream.close();
            //outStream.close();
            //processSocket.close();
            
        } catch (IOException ex) {
            Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NullPointerException ex) {
            Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    
        finally{
            try {
                inStream.close();
                outStream.close();
                processSocket.close();
            } catch (IOException ex) {
                Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
    }
    
    
}

class MarkerSend extends Thread{
    private PrintWriter outStream;
    private Socket processSocket;
    private int pid;
    private boolean FIN_MARKER = false;
    private boolean isCoordinator = false;
    
    public MarkerSend(PrintWriter outStream, Socket processSocket, int pid, boolean FIN_MARKER, boolean isCoordinator){
        this.outStream = outStream;
        this.processSocket = processSocket;
        this.pid = pid;
        this.FIN_MARKER = FIN_MARKER;
        this.isCoordinator = isCoordinator;
    }
    
    @Override
    public void run() {
        synchronized(this) {
            String sendMsg;
            sendMsg = "MARKER";
                
            if(isCoordinator){
                sendMsg += ",TS="+COORDINATOR.L_CLOCK;
                COORDINATOR.L_CLOCK++;
                COORDINATOR.SENT[pid]++;
                outStream.println(sendMsg);
                System.out.println("SENT_MARKER=@COORDINATOR$:"+sendMsg);
            } 
            else{
                sendMsg += ",TS="+PROCESS.L_CLOCK;
                PROCESS.L_CLOCK++;
                PROCESS.SENT[pid]++;
                outStream.println(sendMsg);
                System.out.println("SENT_MARKER=@<"+processSocket.getLocalSocketAddress()+">$:"+sendMsg);
            }
            
            if(FIN_MARKER){
                System.out.println("No more messages will be sent.");
                outStream.close();
            }
        }
    }
}

class MarkerReceive extends Thread{
    private BufferedReader inStream;
    private Socket processSocket;
    private int pid;
    private boolean isCoordinator = false;
    
    public MarkerReceive(BufferedReader inStream, Socket processSocket, int pid, boolean isCoordinator){
        this.inStream = inStream;
        this.processSocket = processSocket;
        this.pid = pid;
        this.isCoordinator = isCoordinator;
    }
    
    @Override
    public void run() {
        synchronized(this){
            try {
                String recvMsg;
                recvMsg = inStream.readLine();
                while(true){
                    if(recvMsg.startsWith("MARKER")){
                        /**
                         * Marker receiving rule.
                         **/
                        int TS = Integer.parseInt(recvMsg.split("TS=")[1]);
                        
                        if(isCoordinator){
                            System.out.println("RECEIVED_MARKER=@COORDINATOR$:"+recvMsg);
                            COORDINATOR.L_CLOCK = Integer.max(COORDINATOR.L_CLOCK, TS+1);
                            COORDINATOR.RECV[pid]++;
                        }
                        else{
                            System.out.println("RECEIVED_MARKER=@<"+processSocket.getRemoteSocketAddress()+">$:"+recvMsg);
                            PROCESS.L_CLOCK = Integer.max(PROCESS.L_CLOCK, TS+1);
                            PROCESS.L_CLOCK++;
                        }
                    }
                }
            } catch (IOException ex) {
                Logger.getLogger(MarkerReceive.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    
    }
}

class PROCESS {

    private Socket processSocket;
    private ServerSocket neighbourSocket = null;
    private Socket newNBSocket;
    public static int NEIGHBOUR_PORT = 2000;
    private static PrintWriter outStream;
    private BufferedReader inStream;
    public static Map<Integer,Integer[]> NEIGHBOUR_LIST;
    public static Map<String, Integer> NB_HOSTNAMES = new HashMap<>();
    public static Map<Integer, Socket> NB_SOCKETS = new HashMap<>();
    public static int L_CLOCK = 0;
    private final int PORT;
    private final String HOST;
    private int MAX_PROC_COUNT;
    public static Random RND = new Random();
    public static int[] MC_VAL;
    public static int RECORD = 0;
    private static int PROC_ID;
    public static Integer[] SENT;
    public static Integer[] RECV;
    public static Integer[] CHANNEL;
    
    public PROCESS(String HOST, int PORT,  Map<Integer,Integer[]> NEIGHBOUR_LIST,int MAX_PROC_COUNT) {
        this.HOST = HOST;
        this.PORT = PORT;
        this.NEIGHBOUR_LIST = NEIGHBOUR_LIST;
        this.MAX_PROC_COUNT = MAX_PROC_COUNT;
        this.SENT = new Integer[this.MAX_PROC_COUNT+1];
        this.RECV = new Integer[this.MAX_PROC_COUNT+1];
        this.CHANNEL = new Integer[this.MAX_PROC_COUNT+1];
        Arrays.fill(SENT, 0);
        Arrays.fill(RECV, 0);
        Arrays.fill(CHANNEL, 0);
        this.MC_VAL = new int[]{0,1,2};
    }
    
    
    private void writeOutToFile(Integer[] sent, Integer[] recv){
        try {
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter("localstate_"+PROC_ID));
            fileWriter.write("SENT "+Arrays.toString(Arrays.copyOfRange(sent, 1, sent.length))+"\n");
            fileWriter.write("RECV "+Arrays.toString(Arrays.copyOfRange(recv, 1, recv.length))+"\n");
            fileWriter.write("CHANNEL "+Arrays.toString(Arrays.copyOfRange(PROCESS.CHANNEL, 1, PROCESS.CHANNEL.length))+"\n");
            fileWriter.close();
        } catch (IOException ex) {
            Logger.getLogger(PROCESS.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public void start() {
    
        try {
            processSocket = new Socket(HOST, PORT);
            inStream = new BufferedReader(new InputStreamReader(processSocket.getInputStream()));
            outStream = new PrintWriter(processSocket.getOutputStream(),true);
            System.out.println("@<"+processSocket.getLocalSocketAddress()+"> has joined <"+HOST+"> on ["+PORT+"].");
            String sendMsg, recvMsg;
            
            //Sending message.
            sendMsg = "REGISTER,TS="+L_CLOCK;
            System.out.println("SENT=@<"+processSocket.getLocalSocketAddress()+">$:"+sendMsg);
            outStream.println(sendMsg);
            L_CLOCK++;
            SENT[1] += 1;
            //--------------------------
            /*
             * Registering process receives PID from
             * Coordinator and stores it locally.
             */
            
            while((recvMsg = inStream.readLine())!=null) {
                if(PROCESS.L_CLOCK % Main.INTERVAL == 0){
                    System.out.println("Sending marker.");
                }
                if(recvMsg.startsWith("PID")){
                    RECV[1]+= 1;
                    System.out.println("RECEVIED=@COORDINATOR$:"+recvMsg);
                    PROC_ID = Integer.parseInt(recvMsg.split("PID=")[1].split(";")[0]);
                    new MarkerReceive(inStream, processSocket, PROC_ID, true).start();
                    //NEIGHBOUR_PORT += PROC_ID;
                    String NB_LIST = recvMsg.split("NB=")[1];
                    for(String NB : NB_LIST.split(";")){
                        if(!NB.startsWith("TS=")){
                            for(String nb: NB.split("/")){
                                if(!nb.startsWith("COORDINATOR")){
                                    NB_HOSTNAMES.put(nb.split("[(\\,\\)]")[1]+nb.split("[(\\,\\)]")[2], Integer.parseInt(nb.split("[(\\,\\)]")[2]));
                                }
                            }
                        }
                    }
                    L_CLOCK++;
                    /**
                     * Start listening on a server socket to
                     * incoming connections from your 
                     * neighbours.
                     **/
                    neighbourSocket = new ServerSocket(NEIGHBOUR_PORT+PROC_ID);
                    neighbourSocket.setReuseAddress(true);
                    neighbourSocket.setSoTimeout(1000*60*60);
                    
                    /**
                     * Sending Hello message to all your neighbours.
                     **/
                    for(Integer neighbourPID: NEIGHBOUR_LIST.get(PROC_ID)){
                        if(neighbourPID!=null && neighbourPID!=1){
                            for(String neighbourHostName : NB_HOSTNAMES.keySet()){
                                if(NB_HOSTNAMES.get(neighbourHostName).equals(neighbourPID)){
                                    neighbourHostName = neighbourHostName.substring(0, neighbourHostName.length()-1);
                                    Socket nb = new Socket(neighbourHostName, NEIGHBOUR_PORT+neighbourPID);
                                    NB_SOCKETS.put(neighbourPID, nb);
                                    PrintWriter pw = new PrintWriter(nb.getOutputStream(), true);
                                    String msg="HELLO,TS="+PROCESS.L_CLOCK;
                                    pw.println(msg);
                                    PROCESS.L_CLOCK++;
                                    PROCESS.SENT[neighbourPID]++;
                                    System.out.println("SENT=@<"+nb.getRemoteSocketAddress()+">$:"+msg);
                                }
                            }
                        }
                    }
                    /**
                     * Receiving hello messages from your neighbours.
                     * Except the coordinator. 
                     **/
                    for(Integer neighbourPID: NEIGHBOUR_LIST.get(PROC_ID)){
                        if(neighbourPID!=null && neighbourPID!=1){
                            newNBSocket = neighbourSocket.accept();
                            
                            new neighbourThreadHandler(
                                    newNBSocket, 
                                    neighbourPID, 
                                    NB_SOCKETS, 
                                    NEIGHBOUR_LIST, 
                                    PROC_ID, 
                                    outStream, 
                                    NB_HOSTNAMES).start();
                        }
                    }
                    
                    if(PROCESS.L_CLOCK == Main.TERMINATE){
                        break;
                    }
                }    
            }
            sendMsg = "READY,TS="+PROCESS.L_CLOCK;
            outStream.println(sendMsg);
            PROCESS.SENT[1]++;
            PROCESS.L_CLOCK++;
            System.out.println("SENT=@<"+processSocket.getLocalSocketAddress()+">$:"+sendMsg);
            int random_sleep = RND.nextInt(5-1)+1;
            while(true) {
                recvMsg = inStream.readLine();
                 if(PROCESS.L_CLOCK % Main.INTERVAL == 0){
                    System.out.println("Sending marker.");
                }
                if(recvMsg.startsWith("COMPUTE")){
                    
                    int TS = Integer.parseInt(recvMsg.split("TS=")[1]);
                    PROCESS.L_CLOCK = Integer.max(PROCESS.L_CLOCK, TS+1);
                    PROCESS.RECV[1]++;
                    System.out.println("RECEIVED=@COORDINATOR$:"+recvMsg);
                    
                    System.out.println("Sleeping for "+random_sleep+" ms.");
                    Thread.sleep(random_sleep);
                    //break;
                    /**
                     * Sending random messages ranging from {0,1,2} 
                     * in count, to randomly selected neighbours.
                     * Specifically to the coordinator, if it is
                     * randomly selected as the neighbour of such 
                     * process. Otherwise, nothing is sent.
                     * For neighbours other than the coordinator,
                     * we handle that in the neighbourThreadHandler
                     * class. Not here.
                     **/
                    sendMsg = "COMPUTE,TS="+PROCESS.L_CLOCK;
                    int rnd_val = RND.nextInt(MC_VAL.length);
                    int rnd_pindex;
                    if(MC_VAL[rnd_val] == 0){
                        System.out.println("No message sent.");
                    }
                    if(MC_VAL[rnd_val] == 1){
                        System.out.println("Sending 1 message.");
                        rnd_pindex = new Random().nextInt(NEIGHBOUR_LIST.get(PROC_ID).length-1)+1;
                        if(NEIGHBOUR_LIST.get(PROC_ID)[rnd_pindex] == 1){
                            outStream.println(sendMsg);
                            PROCESS.L_CLOCK++;
                            PROCESS.SENT[1]++;
                            System.out.println("SENT=@COORDINATOR$:"+sendMsg);
                        }
                    }
                    if(MC_VAL[rnd_val] == 2){
                        System.out.println("Sending 2 messages.");
                        for(int i=1;i<=2;i++){
                            rnd_pindex = new Random().nextInt(NEIGHBOUR_LIST.get(PROC_ID).length-1)+1;
                            if(NEIGHBOUR_LIST.get(PROC_ID)[rnd_pindex] == 1){
                                outStream.println(sendMsg);
                                PROCESS.L_CLOCK++;
                                PROCESS.SENT[1]++;
                                System.out.println("SENT=@COORDINATOR$:"+sendMsg);
                            }
                        }
                    
                    }   
                }
                if(PROCESS.L_CLOCK == Main.TERMINATE){
                    break;
                }
                
            }
            
            
        } catch (IOException ex) {
            Logger.getLogger(PROCESS.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(PROCESS.class.getName()).log(Level.SEVERE, null, ex);
        } 
        
        finally{
            try {
                outStream.close();
                inStream.close();
                processSocket.close();
            } catch (IOException ex) {
                Logger.getLogger(PROCESS.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    
    }
    

}

/**
 * Class to handle every incoming neighbour request.
 * 
 **/
class neighbourThreadHandler extends Thread {
    
    private Socket neighbourSocket;
    private BufferedReader inStream;
    private Map<Integer, Socket> NB_SOCKETS;
    private Map<Integer, Integer[]> NEIGHBOUR_LIST;
    private int pid;
    private int PROC_ID;
    private int[] MC_VAL = new int[]{0,1,2};
    private PrintWriter outStream;
    private Map<String, Integer> NB_HOSTNAMES;
    
    
    
    public neighbourThreadHandler(
            Socket neighbourSocket, 
            int pid, 
            Map<Integer,Socket> NB_SOCKETS, 
            Map<Integer, Integer[]> NEIGBOUR_LIST,
            int ownPID,
            PrintWriter ownPW, 
            Map<String, Integer> NB_HOSTNAMES){
        this.neighbourSocket = neighbourSocket;
        this.pid = pid;
        this.NB_SOCKETS = NB_SOCKETS;
        this.NEIGHBOUR_LIST = NEIGBOUR_LIST;
        this.PROC_ID = ownPID;
        this.outStream = ownPW;
        this.NB_HOSTNAMES = NB_HOSTNAMES;
    }
    private void sendMsgToNB(int neighbourPID, String msg, PrintWriter pw){
    /**
         * Case 1 : If the coordinator is a neighbour.
         **/
        if(neighbourPID == 1){
            msg += ",TS="+PROCESS.L_CLOCK;
            pw.println(msg);
            System.out.println("SENT=@COORDINATOR$:"+msg);
            PROCESS.SENT[1]++;
            PROCESS.CHANNEL[1]++;
            PROCESS.L_CLOCK++;
        } 
        /**
         * Case 2 : If the coordinator is not a neighbour.
         **/
        else {
            for(String neighbourHostName: NB_HOSTNAMES.keySet()){
                if(NB_HOSTNAMES.get(neighbourHostName).equals(neighbourPID)){
                    try {
                        PrintWriter pwriter = new PrintWriter(NB_SOCKETS.get(neighbourPID).getOutputStream(), true);
                        msg += ",TS="+PROCESS.L_CLOCK;
                        pwriter.println(msg);
                        pwriter.flush();
                        PROCESS.L_CLOCK++;
                        PROCESS.SENT[neighbourPID]++;
                        System.out.println("SENT=@<"+NB_SOCKETS.get(neighbourPID).getRemoteSocketAddress()+">$:"+msg);
                    } catch (IOException ex) {
                        Logger.getLogger(PROCESS.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
    }
    private void randomSend(String msg){
        int rnd_val = new Random().nextInt(MC_VAL.length);
        int rnd_pindex;
        
        if(MC_VAL[rnd_val] == 0){
            System.out.println("No message sent.");
        }
        if(MC_VAL[rnd_val] == 1){
            System.out.println("Sending 1 message.");
            rnd_pindex = new Random().nextInt(NEIGHBOUR_LIST.get(PROC_ID).length-1)+1;
            sendMsgToNB(NEIGHBOUR_LIST.get(PROC_ID)[rnd_pindex], msg , outStream);
        }
        if(MC_VAL[rnd_val] == 2){
            System.out.println("Sending 2 messages.");
            for(int i=1;i<=2;i++){
                rnd_pindex = new Random().nextInt(NEIGHBOUR_LIST.get(PROC_ID).length-1)+1;
                sendMsgToNB(NEIGHBOUR_LIST.get(PROC_ID)[rnd_pindex], msg, outStream);
            }
        }
    }
    
    @Override
    public void run() {
        
            try {
                inStream = new BufferedReader(new InputStreamReader(neighbourSocket.getInputStream()));
                String recvMsg;
                
                while(true){
                    recvMsg = inStream.readLine();
                    new MarkerReceive(inStream, neighbourSocket, pid, false).start();
                    if (recvMsg.startsWith("HELLO")){
                        int TS = Integer.parseInt(recvMsg.split("TS=")[1]);
                        PROCESS.L_CLOCK = Integer.max(PROCESS.L_CLOCK,TS+1);
                        PROCESS.RECV[pid]++;
                        PROCESS.CHANNEL[pid]--;
                        System.out.println("RECEIVED=@<"+neighbourSocket.getRemoteSocketAddress()+">$:"+recvMsg);
                    }
                    if(recvMsg.startsWith("COMPUTE")){
                        int TS = Integer.parseInt(recvMsg.split("TS=")[1]);
                        PROCESS.L_CLOCK = Integer.max(PROCESS.L_CLOCK,TS+1);
                        PROCESS.RECV[pid]++;
                        PROCESS.CHANNEL[pid]--;
                        System.out.println("RECEIVED=@<"+neighbourSocket.getRemoteSocketAddress()+">$:"+recvMsg);
                        int random_sleep = new Random().nextInt(5-1)+1;
                        System.out.println("Sleeping for "+random_sleep+" ms.");
                        Thread.sleep(random_sleep);
                        randomSend("COMPUTE");
                    }
                    randomSend("COMPUTE");
                    if(PROCESS.L_CLOCK == Main.TERMINATE){
                        break;
                    }
                }

            } catch (IOException ex) {
                Logger.getLogger(neighbourThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(neighbourThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
            } 
            finally{
                try {
                    inStream.close();
                    neighbourSocket.close();
                } catch (IOException ex) {
                    Logger.getLogger(neighbourThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
    }
}
