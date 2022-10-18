import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Semaphore;



class ProcessHandler {
    String process_name;
    Integer port = null;
    ServerSocket server;
    public ProcessHandler other_processess[];
    String address;
    boolean critical_section_access;
    boolean critical_section_request;
    String curr_proc_req_timestamp;
    Semaphore mutex = new Semaphore(1);
    Semaphore process_exe = new Semaphore(1);
    Queue<Socket> deferred_req;
    // Queue<DataOutputStream> deferred_req;
    HashMap<String, Integer> database;
    Queue<String> req_timestamp_queue;

    public ProcessHandler (Integer server_port, String process_name, HashMap<String, Integer> database) throws IOException {
        this.process_name = process_name;
        this.port = server_port;
        this.server = new ServerSocket(server_port);
        this.address = "127.0.0.1";
        this.critical_section_access = false;
        this.critical_section_request = false;
        this.deferred_req = new LinkedList<Socket>();
        this.req_timestamp_queue = new LinkedList<String>();
        this.curr_proc_req_timestamp = "200";
        this.database = database;


    }

    public void server_on() throws IOException {
        while(true) {

            synchronized (this) {
                System.out.println(this.process_name+ " "+this.port +" Server waiting for client ...");
                Socket socket = server.accept();
                System.out.println("0 Connection establish for client ...");

                /**/

                DataInputStream client_input = new DataInputStream(socket.getInputStream());
                DataOutputStream client_output = new DataOutputStream(socket.getOutputStream());
                String req_timestamp=  client_input.readUTF();
                String req_process_name =  client_input.readUTF();

                /*
                boolean res = true;

                if (critical_section_access == true)
                res = false;
                else if (critical_section_request == true) {
                if (Integer.parseInt(this.curr_proc_req_timestamp) >  Integer. parseInt(req_timestamp))
                res = false;
                else if(Integer.parseInt(this.curr_proc_req_timestamp) ==  Integer. parseInt(req_timestamp))
                if (req_process_name.compareTo(this.process_name) > 0) {
                System.out.println("compare"+req_process_name+"to"+this.process_name+": "+req_process_name.compareTo(this.process_name));
                res = false;
                }

                }*/

                boolean res = critical_section_access == false &&
                        (critical_section_request == false ||
                                (critical_section_request == true && Integer. parseInt(this.curr_proc_req_timestamp) >  Integer. parseInt(req_timestamp)));
//                System.out.println("Process "+this.process_name+"Receives req from "+req_process_name+ "res : "+res);
                if (res) {
//                                ((Integer. parseInt(this.curr_proc_req_timestamp) ==  Integer. parseInt(req_timestamp) && req_process_name.compareTo(this.process_name) < 0) ||
//                                Integer. parseInt(this.curr_proc_req_timestamp) >  Integer. parseInt(req_timestamp)))))
                    System.out.println("Process " + this.process_name + "send REPLY to" + req_process_name+"port :"+this.port);
                    client_output.writeUTF("REPLY from " + this.port+"to "+req_process_name);
                    socket.close();

                } else {
                    /* else put the request in queue */

                    deferred_req.add(socket);
                    //deferred_req.add(client_output);
                    System.out.println("Process "+this.process_name+"Defered req "+req_process_name);
                    //     System.out.println("Defered prcess " + req_process_name+"by"+this.process_name+ "param :" +this.process_name+":"+this.curr_proc_req_timestamp +"  "+req_process_name+" :"+req_timestamp);
                }

            }

        }

    }

    public void assign_other_processes(ProcessHandler   p1, ProcessHandler p2) {
        this.other_processess = new ProcessHandler[2];
        this.other_processess[0] = p1;
        this.other_processess[1] = p2;
    }

    public void set_timestamp(String req_timestamp) throws InterruptedException {
        mutex.acquire();
        if (this.curr_proc_req_timestamp.equals("200")) {
            this.curr_proc_req_timestamp = req_timestamp;
            this.critical_section_request = true;
        } else
            this.req_timestamp_queue.add(req_timestamp);
        mutex.release();

    }

    /* Process Handler client*/
    public void exec_req(String p_name, String transaction, String user, String value, String transac) throws InterruptedException, IOException {
        process_exe.acquire();
        /* send request to everyone create socket */
        ProcessHandler process1 = this.other_processess[0];
        ProcessHandler process2 = this.other_processess[1];
        // System.out.println("Requ from "+p_name);


        Thread t1 = new Thread(() -> {

            try {

                System.out.println("curr_process " +this.process_name+" wait from"+ process1.process_name);
                //Socket socket = new Socket(process1.address, process1.port);
                Socket socket = new Socket(InetAddress.getLocalHost(), process1.port);
                DataInputStream server_input = new DataInputStream(socket.getInputStream());
                DataOutputStream server_output = new DataOutputStream(socket.getOutputStream());
                //process1.recv_req(this.curr_proc_req_timestamp, this.process_name);
                server_output.writeUTF(this.curr_proc_req_timestamp);
                server_output.writeUTF(this.process_name);

                String res = server_input.readUTF();
                System.out.println("curr_process " +this.process_name+" recev reply from"+ process1.process_name+" "+res);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        Thread t2 = new Thread(() -> {
            /**/
            try {
                System.out.println("curr_process " +this.process_name+" wait from"+ process2.process_name);
                // Socket socket = new Socket(process2.address, process2.port);
                Socket socket = new Socket(InetAddress.getLocalHost(), process2.port);
                DataInputStream server_input = new DataInputStream(socket.getInputStream());
                DataOutputStream server_output = new DataOutputStream(socket.getOutputStream());
                server_output.writeUTF(this.curr_proc_req_timestamp);
                server_output.writeUTF(this.process_name);
                //     process2.recv_req(this.curr_proc_req_timestamp, this.process_name);
                String res = server_input.readUTF(); //wait for REPLY
                System.out.println("curr_process " +this.process_name+" recev reply from"+ process2.process_name+" "+res);
                // System.out.println(res);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        System.out.println("Requ from "+p_name+"Receiv reply from all others");
        mutex.acquire();
        critical_section_access = true;
        mutex.release();



        /* Access critical section */
        String output;
        switch (transaction) {
            case "CheckBalance":
                System.out.println("**********CheckBalance : "+user + " has $"+this.database.get(user)+transac);
                String write_file_name = "output.txt";
                BufferedWriter wr = new BufferedWriter(new FileWriter(write_file_name, true));
                wr.write(user + " has $"+this.database.get(user)+"\n");
                wr.close();
                break;
            case "DepositCash":
                this.database.put(user, this.database.get(user) + Integer.valueOf(value));
                System.out.println("**********DepositCash : "+ user +" has "+ this.database.get(user)+transac);
                break;
            case "ApplyInterest":
                int inc = (this.database.get(user) * Integer.valueOf(value)) / 100;
                this.database.put(user, this.database.get(user) + inc);
                System.out.println("**********ApplyInterest : "+ user +" has "+ this.database.get(user)+transac);
                break;
            case "WithdrawCash":
                this.database.put(user, this.database.get(user) - Integer.valueOf(value));
                System.out.println("**********WithdrawCash : "+ user +" has "+ this.database.get(user)+transac);
                break;
            default:
                System.out.println("**************** INVALID TRANSACTION COMMAND ******************"+transac);

        }

        mutex.acquire();
        if (!req_timestamp_queue.isEmpty())
        {
            critical_section_request = true;
            curr_proc_req_timestamp = req_timestamp_queue.remove();
        } else
            critical_section_request = false;
        critical_section_access = false;
        mutex.release();
        System.out.println("Execution of "+this.process_name+"has been compleyed");

        while(!deferred_req.isEmpty()) {
            Socket socket =deferred_req.remove();
            DataOutputStream client_output = new DataOutputStream(socket.getOutputStream());
            client_output.writeUTF("REPLY2 from "+this.port);
        }

        process_exe.release();
    }
}

class Transcation {
    String processID;
    String transaction;
    String timestamp;
    String user;
    String value;
    String operation;

    public void initialize(String processID, String transaction, String timestamp, String user, String value, String operation)
    {
        this.processID = processID;
        this.transaction = transaction;
        this.timestamp = timestamp;
        this.user = user;
        this.value = value;
        this.operation = operation;
    }
}

public class Main {
    static HashMap<String, Integer> database;
    static ProcessHandler[] processes;
    static HashMap<String, ProcessHandler> map;
    static Semaphore req_exe = new Semaphore(1);
    static int clock_var =0;
    String input_transactions[];
    static List<Transcation> T = new ArrayList<Transcation>();

    public static Comparator<Transcation> PriorityComparator
            = new Comparator<Transcation>() {

        public int compare(Transcation t1, Transcation t2) {
            if (Integer.parseInt(t1.timestamp) == Integer.parseInt(t2.timestamp)) {
                return t1.processID.compareTo(t2.processID);
            } else
                return (Integer.parseInt(t1.timestamp) < Integer.parseInt(t2.timestamp)) ? -1 : 1;
        }

    };
    /* Receives operations at particular timestamp */
    public static void processScheduler(Transcation trans) throws InterruptedException {
        req_exe.acquire();
        System.out.println("set timestamp for "+trans.processID + "to "+trans.timestamp);
        //map.get(trans.processID).set_timestamp(trans.timestamp);

        map.get(trans.processID).set_timestamp(String.valueOf(clock_var));
        clock_var++;
        req_exe.release();
        System.out.println("curr_p : "+ trans.processID);

        new Thread() {
            public void run() {
                try {
                    map.get(trans.processID).exec_req(trans.processID,
                            trans.transaction, trans.user,trans.value, trans.operation);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();

    }

    public static void TransactionHandler() throws InterruptedException {

        for (int i = 0; i < T.toArray().length; i++) {
            processScheduler(T.get(i));
        }
    }

    public static void databaseInitialization() {
        database = new HashMap<>();
        database.put("A", 100);
        database.put("B", 100);
        database.put("C", 100);
    }

    public static void processInitialization() throws IOException {

        processes = new ProcessHandler[3];

        ProcessHandler p1 = new ProcessHandler(5001, "P1", database);
        ProcessHandler p2 = new ProcessHandler(5002, "P2", database);
        ProcessHandler p3 = new ProcessHandler(5003, "P3", database);

        p1.assign_other_processes(p2, p3);
        p2.assign_other_processes(p1, p3);
        p3.assign_other_processes(p1, p2);

        processes[0] = p1;
        processes[1] = p2;
        processes[2] = p3;
        /* Start  server: TBD: move this in constructor */
        for (int i = 0; i < processes.length; i++) {
            /* Run server */

            int finalI = i;
            new Thread() {
                public void run() {
                    try {
                        processes[finalI].server_on();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }

        map = new HashMap<>();
        map.put("P1", p1);
        map.put("P2", p2);
        map.put("P3", p3);


    }

    public static void main(String[] args) throws IOException, InterruptedException {

        readTransaction();
        databaseInitialization();
        processInitialization();
        TransactionHandler();
    }

    private static void readTransaction() throws IOException {

        String read_file_name = "input.txt";
        BufferedReader br = new BufferedReader(new FileReader(read_file_name));


        String data;


        while((data = br.readLine()) != null) {
            String processID = data.substring(0, 2);
            String transactions_by_process[] = data.substring(3, data.length()).replaceAll(" ", "").split("\\),");


            for (int i = 0; i < transactions_by_process.length;i++) {
                Transcation t = new Transcation();
                t.processID =processID;
                String transac[] = transactions_by_process[i].split("[(,)]");

                t.transaction = transac[0];
                t.timestamp = transac[1];
                t.user = transac[2];
                t.operation = transactions_by_process[i];
                if (transac.length == 4) {
                    t.value = transac[3].replace("$", "").replace("%", "");
                } else
                    t.value = "";
                T.add(t);
            }
        };
        T.sort(PriorityComparator);

        for(int k = 0; k <T.toArray().length;k++)
        {
            Transcation temp = T.get(k);
            System.out.println(temp.processID+ " "+temp.timestamp+" "+temp.transaction+" "+temp.user+" " +temp.value);

        }
    }


}

