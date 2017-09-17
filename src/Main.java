/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
    private static String FILE_PATH;
    private static BufferedReader BR;
    private static String LINE;
    private static String HOST = "";
    private static int PORT = 1234;
    private static int MAX_PROC_COUNT = 1;
    private static final Map<Integer, Integer[]> NEIGHBOUR_LIST = new HashMap<>();
    
    
    public static void main(String[] args) throws IOException {
        
        FILE_PATH = new File("").getAbsolutePath();
        BR = new BufferedReader(new FileReader(FILE_PATH+"/res/dsConfig"));
        String nb_line;
        String[] processList;
        Integer[] processNeighbours;
        while((LINE = BR.readLine()) != null) {
            if (LINE.startsWith("COORDINATOR")) {
                HOST = LINE.split("COORDINATOR")[1].trim();
            }
            if (LINE.startsWith("NUMBER")){
                MAX_PROC_COUNT = Integer.parseInt(LINE.split("NUMBER OF PROCESSES")[1].trim());
                
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

        if (args.length == 1) {
        
            if(args[0].equalsIgnoreCase("-c")) {
                new COORDINATOR(PORT, MAX_PROC_COUNT).start();
            } 
        } else {
                new PROCESS(HOST, PORT, NEIGHBOUR_LIST).start();
            
        }

    }
    
}


class COORDINATOR {

    private static ServerSocket coordinatorSocket = null;
    private static Socket processSocket = null;
    private int MAX_PROC_COUNT;
    private static int PROC_INDEX = 1;
    
    private Map<Socket, Integer> PROCESS_IDS = new HashMap<>();
    
    private int PORT = 1234;
    /*
     * Initialize the COORDINATOR constructor with all the passed
     * parameters. 
     */
    public COORDINATOR(int PORT, int MAX_PROC_COUNT) {
        this.PORT = PORT;
        this.MAX_PROC_COUNT = MAX_PROC_COUNT;
    }
    

    public void start() {
    
        try {
            coordinatorSocket = new ServerSocket(PORT);
            coordinatorSocket.setReuseAddress(true);
        } catch (IOException ex) {
            Logger.getLogger(COORDINATOR.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println("COORDINATOR started at <"+coordinatorSocket.getLocalSocketAddress()+">.");

        //Continously accept new connections form individual clients.
        //Pass every accepted socket into a handler thread.
        //Every passed socket then can communicate separately with the Server (Coordinator).
        while (true) {
            
            try {
                processSocket = coordinatorSocket.accept();
                PROCESS_IDS.put(processSocket, PROC_INDEX);
                new processThreadHandler(processSocket,PROCESS_IDS).start();
                PROC_INDEX++;
                if (PROC_INDEX == MAX_PROC_COUNT) {
                    PrintStream outStream = new PrintStream(processSocket.getOutputStream());
                    outStream.println("Reached MAX limit of Processes.");
                    outStream.close();
                    //coordinatorSocket.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(COORDINATOR.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
}


class processThreadHandler extends Thread {
    private DataInputStream inStream = null;
    private Socket processSocket = null;
    private PrintStream outStream = null;
    private Map<Socket,Integer> PROCESS_IDS;
  
    public processThreadHandler(Socket processSocket, Map<Socket, Integer> PROCESS_IDS) {
        this.processSocket = processSocket;
        this.PROCESS_IDS = PROCESS_IDS;
    }
    
    @Override
    public void run() {
        
        try {
            inStream = new DataInputStream(processSocket.getInputStream());
            outStream = new PrintStream(processSocket.getOutputStream());
            System.out.println("@PROCESS<"+processSocket.getRemoteSocketAddress()+"> has joined.");
            
            String sendMsg, recvMsg;
            while(true) {
                recvMsg = inStream.readLine();
                System.out.println("@PROCESS<"+processSocket.getRemoteSocketAddress()+">::"+recvMsg);
                
                if(recvMsg.equalsIgnoreCase("SEND")) {
                    sendMsg = "RECV";
                    System.out.println("@COORDINATOR::"+sendMsg);
                    outStream.println(sendMsg);
                }
                if (recvMsg.equalsIgnoreCase("ACK")) {
                    break;
                }
            }
            inStream.close();
            outStream.close();
            //processSocket.close();
            
        } catch (IOException ex) {
            Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    
}


class PROCESS extends Thread {

    private Socket processSocket = null;
    private PrintStream outStream = null;
    private DataInputStream inStream = null;
    private Map<Integer,Integer[]> NEIGHBOUR_LIST;
    private final int PORT;
    private final String HOST;
    
    public PROCESS(String HOST, int PORT, Map<Integer,Integer[]> NEIGHBOUR_LIST) {
        this.HOST = HOST;
        this.PORT = PORT;
        this.NEIGHBOUR_LIST = NEIGHBOUR_LIST;
    }
    
    @Override
    public void run() {
    
        try {
            processSocket = new Socket(HOST,PORT);
            inStream = new DataInputStream(processSocket.getInputStream());
            outStream = new PrintStream(processSocket.getOutputStream());
            System.out.println("@PROCESS<"+processSocket.getLocalSocketAddress()+"> has joined <"+HOST+"> on ["+PORT+"].");
            String sendMsg, recvMsg;

            while(true) {
                sendMsg = "SEND";
                System.out.println("@PROCESS<"+processSocket.getLocalSocketAddress()+">::"+sendMsg);
                outStream.println(sendMsg);
                recvMsg = inStream.readLine();
                System.out.println("@COORDINATOR::"+recvMsg);
                if(recvMsg.equalsIgnoreCase("RECV")) {
                    sendMsg = "ACK";
                    outStream.println(sendMsg);
                    break;
                }
            }
            
            outStream.close();
            inStream.close();
            //processSocket.close();
            
        } catch (IOException ex) {
            Logger.getLogger(PROCESS.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    
    }
    

}

