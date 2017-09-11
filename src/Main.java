/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Ashwin
 */
public class Main{

    public static ServerSocket server;
    public static boolean server_state = false;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        
        /*
         * Open a file handler to read from config file named "dsConfig".
         * Read first 2 lines to get the coordinator host and max number of processes.
         * Get port number from commandline argument[1].
         */
        String file_path = new File("").getAbsolutePath();
        BufferedReader br = new BufferedReader(new FileReader(file_path+"/res/dsConfig"));
        String line, HOST = "";
        int MAX_PROC_COUNT = 1;
        int port;
        while((line = br.readLine()) != null) {
            if (line.startsWith("COORDINATOR")) {
                HOST = line.split("COORDINATOR")[1].trim();
            }
            if (line.startsWith("NUMBER")){
                MAX_PROC_COUNT = Integer.parseInt(line.split("NUMBER OF PROCESSES")[1].trim());
            }
        }
        /*
         * If the number of arguments are equal to 2.
         * then the first is the option to run the coordinator,
         * and the second one is the port number.
         * Then start the COORDINATOR class.
         */
        if (args.length == 2) {
            
            if (args[0].equalsIgnoreCase("-c")) {
                port = Integer.parseInt(args[1]);
                System.out.println("Starting the COORDINATOR at HOST="+HOST);
                new COORDINATOR(port, MAX_PROC_COUNT).start();
            }
        }
        /*
         * Otherwise if there is only argument, 
         * then that argument is the port number.
         * With this info start the PROCESS class.
         */
        else if (args.length == 1) {
            System.out.println("Starting PROCESS.");
            port = Integer.parseInt(args[0]);
            System.out.println("Connecting to COORDINATOR on PORT="+port);
            new PROCESS(HOST, port).init();
        }
        
        
    }
    
    
}


class COORDINATOR {

    private static ServerSocket coordinatorSocket = null;
    private static Socket processSocket = null;
    private static int MAX_PROC_COUNT;
    private static int PROC_INDEX = 0;
    private ArrayList<processThreadHandler> PROC_THREADS = new ArrayList<>();
    
    
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
                PROC_THREADS.add(new processThreadHandler(processSocket,PROC_THREADS));
                PROC_THREADS.get(PROC_INDEX).init();
                
                if (PROC_INDEX == MAX_PROC_COUNT) {
                    PrintStream outStream = new PrintStream(processSocket.getOutputStream());
                    outStream.println("Reached MAX limit of Processes.");
                    outStream.close();
                    coordinatorSocket.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(COORDINATOR.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
}


class processThreadHandler extends Thread {
    private DataInputStream inStream = null;
    private BufferedReader keyInput = null;
    private PrintStream outStream = null;
    private Socket processSocket = null;
    private ArrayList<processThreadHandler> PROC_THREADS;
    private int MAX_PROC_COUNT = 1;
  
    public processThreadHandler(Socket processSocket,ArrayList<processThreadHandler> PROC_THREADS) {
        this.processSocket = processSocket;
        this.PROC_THREADS = PROC_THREADS;
        this.MAX_PROC_COUNT = PROC_THREADS.size();
    }
    public processThreadHandler() {}
    
    public void init() {
        
        try {
            inStream = new DataInputStream(processSocket.getInputStream());
            System.out.println("@PROCESS<"+processSocket.getRemoteSocketAddress()+"> has joined.");
            
            for(int i=0;i<MAX_PROC_COUNT;i++){
                System.out.println("["+(i+1)+"]"+" PROCESS<"+PROC_THREADS.get(i).toString()+">");
            }
            String line;
            while(true) {
                line = inStream.readLine();
                System.out.println("@PROCESS<"+processSocket.getRemoteSocketAddress()+">::"+line);
                new Thread(new processThreadHandler()).run();
                if (line.equalsIgnoreCase("q")) {
                    break;
                }
            }
            inStream.close();
            processSocket.close();
            
        } catch (IOException ex) {
            Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    @Override
    public void run() {
    
        
        try {

            outStream = new PrintStream(processSocket.getOutputStream());
            keyInput = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while(true) {
                System.out.println("@COORDINATOR::");
                line = keyInput.readLine();
                outStream.println(line);
                if (line.equalsIgnoreCase("q")) {
                    break;
                }
            }
            
            /*
             * Cleaning up, to close current sockets and open streams.
             */
            outStream.close();
            keyInput.close();
            processSocket.close();
            
        } catch (IOException e) {
            Logger.getLogger(processThreadHandler.class.getName()).log(Level.SEVERE, null, e);
        }
        
    }
}

class PROCESS extends Thread {

    private static Socket processSocket = null;
    private static PrintStream outStream = null;
    private static DataInputStream inStream = null;
    private static BufferedReader keyInput = null;

    
    /*
     * Default values of PORT and HOST.
     */
    private int PORT = 1234;
    private String HOST = "localhost";
    
    public PROCESS(String HOST, int PORT) {
        this.HOST = HOST;
        this.PORT = PORT;
    }
    
    public PROCESS() {}
    
    public void init() {
    
        try {
            processSocket = new Socket(HOST,PORT);
            keyInput = new BufferedReader(new InputStreamReader(System.in));
            outStream = new PrintStream(processSocket.getOutputStream());
            System.out.println("@PROCESS<"+processSocket.getLocalSocketAddress()+"> has joined <"+HOST+"> on ["+PORT+"].");
            String line;
            while(true) {
                System.out.println("@PROCESS<"+processSocket.getLocalSocketAddress()+">::");
                line = keyInput.readLine();
                outStream.println(line);
                System.out.println("before starting listener thread");
                new Thread(new PROCESS()).run();
                System.out.println("after listener thread");
                if(line.equalsIgnoreCase("q")) {
                    break;
                }
            }
            
            outStream.close();
            keyInput.close();
            processSocket.close();
            
        } catch (IOException ex) {
            Logger.getLogger(PROCESS.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    
    }
    
    @Override
    public void run() {
        try {

            
            inStream = new DataInputStream(processSocket.getInputStream());
            String line;
            while(true) {
                line = inStream.readLine();
                System.out.println("@COORDINATOR::"+line);
                if (line.equalsIgnoreCase("q")) {
                    break;
                }
            }
            
            inStream.close();
            processSocket.close();
            
        } catch (IOException ex) {
            Logger.getLogger(PROCESS.class.getName()).log(Level.SEVERE, null, ex);
        }
        
       
    }

}