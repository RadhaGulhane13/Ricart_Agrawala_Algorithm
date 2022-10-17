import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
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

                boolean res = critical_section_access == false &&
                        (critical_section_request == false ||
                                (critical_section_request == true && Integer. parseInt(this.curr_proc_req_timestamp) >  Integer. parseInt(req_timestamp)));
                System.out.println("Process "+this.process_name+"Receives req from "+req_process_name+ "res : "+res);
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


    /* Process Handler Server */
    public void recv_req(String req_timestamp, String req_process_name) throws IOException, InterruptedException {
        mutex.acquire();
        Socket socket = server.accept();
        DataInputStream client_input = new DataInputStream(socket.getInputStream());
        DataOutputStream client_output = new DataOutputStream(socket.getOutputStream());


        System.out.println("Requ from :"+req_process_name+"to"+this.process_name);
        boolean res = critical_section_access == false &&
                (critical_section_request == false ||
                        (critical_section_request == true && Integer. parseInt(this.curr_proc_req_timestamp) >  Integer. parseInt(req_timestamp)));
        System.out.println("Process "+this.process_name+"Receives req from "+req_process_name+ "res : "+res);
        if (res) {
//                                ((Integer. parseInt(this.curr_proc_req_timestamp) ==  Integer. parseInt(req_timestamp) && req_process_name.compareTo(this.process_name) < 0) ||
//                                Integer. parseInt(this.curr_proc_req_timestamp) >  Integer. parseInt(req_timestamp)))))
            System.out.println("Process " + this.process_name + "send REPLY to" + req_process_name+"port :"+this.port);
            client_output.writeUTF("REPLY from " + this.port+"to "+req_process_name);

        } else {
            /* else put the request in queue */

            deferred_req.add(socket);
            //deferred_req.add(client_output);
            System.out.println("Process "+this.process_name+"Defered req "+req_process_name);
            //     System.out.println("Defered prcess " + req_process_name+"by"+this.process_name+ "param :" +this.process_name+":"+this.curr_proc_req_timestamp +"  "+req_process_name+" :"+req_timestamp);
        }
        // mutex.release();

        /* get process_clock_timestamp and other information related to transaction */
        /* check if ricart can send the REPLY */
        /* Send REPLY if possible */
        /* else put the request in queue*/

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
        /* mutex update request_timestamp = clock_timestamp */
        /* wait the REPLY from everyone*/

        /* Access critical section */
        String output;
        switch (transaction) {
            case "CheckBalance":
                System.out.println("**********CheckBalance : "+user + " has $"+this.database.get(user)+transac);
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
        /* send request to other two processess*/
//        (new Thread() {
//            public void run() {
//                /**/
//            }
//        }).start();
        /* ACCESS CRITICAL SECTION */
        /* send REPLY to all defereed process*/

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
//            DataOutputStream client_output = deferred_req.remove();
            client_output.writeUTF("REPLY2 from "+this.port);
        }
        System.out.println("process :" + p_name+"\n");

        process_exe.release();
    }

//    public void server_start() throws IOException {
//        server = new ServerSocket(port);
//    }

}
/*
class ThreadHandler extends Thread {
    ProcessHandler process_handler;
    String process_id;
    String timestamp;
    String transaction;
    String user, String value

    public ThreadHandler(ProcessHandler process_handler, String process_id, String timestamp, String transaction, String user, String value) {
        this.process_handler = process_handler;
        this.process_id = process_id;
        this.timestamp = timestamp;
        String transaction, String user, String value
    }

    public void run()
    {
        this.process_handler.exec_req(this.process_id);
    }
}
*/


public class Main {


    public static void main(String[] args) throws IOException, InterruptedException {
//        String t = "P1: CheckBalance (1, A)".replaceAll(" ", "");
//
//        String temp[] = t.split("[:,()]");
//
//        for (int i =0; i < temp.length;i++)
//            System.out.println(temp[i] + " "+temp.length);

        Integer clock_var = 0;

        Integer processes = 3;
        Semaphore req_exe = new Semaphore(1);
        /*
        P1: DepositCash (4, A, $20), ApplyInterest (57, C, 10%), CheckBalance (200, A)
– P2: WithdrawCash (4, C, $30), DepositCash (63, B, $40), CheckBalance (200, B)
– P3: ApplyInterest (2, B, 10%), WithrawCash (68, A, $10), CheckBalance (200, C) */

//        String Transaction_P1 = "P1: DepositCash (4, A, $20), ApplyInterest (57, C, 10%), CheckBalance (200, A)";
//        String Transaction_P2 = "P2: WithdrawCash (4, C, $30), DepositCash (63, B, $40), CheckBalance (200, B)";
//
//        String Transaction_P3 = "P3: ApplyInterest (2, B, 10%), WithrawCash (68, A, $10), CheckBalance (200, C)";


//        String sorted_transactions[] = {"P3: ApplyInterest (2, B, 10%)", "P1: DepositCash (4, A, $20)",
//                                        "P2: WithdrawCash (4, C, $30)"};
        // String sorted_transactions[] = {"P1: CheckBalance (1, A)", "P2: CheckBalance (10, B)", "P3: CheckBalance (100, C)"};
        //String sorted_transactions[] = {"P3: ApplyInterest (2, B, 10%)", "P1: DepositCash (4, A, $20)"};

        String sorted_transactions[] = {"P3: ApplyInterest (2, B, 10)","P1: DepositCash (4, A, 20)", "P2: WithdrawCash (4, C, 30)", "P1: ApplyInterest (57, C, 10)","P2: DepositCash (63, B, 40)", "P3: WithdrawCash (68, A, 10)", "P1: CheckBalance (1, A)", "P2: CheckBalance (10, B)", "P3: CheckBalance (100, C)"};
        //    String sorted_transactions[] = {"P3: ApplyInterest (2, B, 10)","P1: DepositCash (4, A, 20)", "P2: WithdrawCash (4, C, 30)"};
        HashMap<Integer, String[]> timestamp_str = new HashMap<>();
        timestamp_str.put(2, new String[]{"P3: ApplyInterest (2, B, 10)"});
        timestamp_str.put(4, new String[]{"P1: DepositCash (4, A, 20)", "P2: WithdrawCash (4, C, 30)"});


        HashMap<String, Integer> database = new HashMap<>();
        database.put("A", 100);
        database.put("B", 100);
        database.put("C", 100);

        ProcessHandler p1 = new ProcessHandler(5001, "P1", database);
        ProcessHandler p2 = new ProcessHandler(5002, "P2", database);
        ProcessHandler p3 = new ProcessHandler(5003, "P3", database);

        p1.assign_other_processes(p2, p3);
        p2.assign_other_processes(p1, p3);
        p3.assign_other_processes(p1, p2);

        /* Run server */
        new Thread() {
            public void run() {


                try {
                    p1.server_on();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            public void run() {


                try {
                    p2.server_on();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            public void run() {


                try {
                    p3.server_on();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        HashMap<String, ProcessHandler> map = new HashMap<>();
        map.put("P1", p1);
        map.put("P2", p2);
        map.put("P3", p3);

        /* first sort all processes and at each clock check how many processes are there, accord*/
        int curr_tranc_idx = 0;

        for (int i = 0; i < sorted_transactions.length; i++) {
            String trans = sorted_transactions[i];
            String ele[] = sorted_transactions[i].replaceAll(" ", "").split("[:,()]");
            String process_id = ele[0];
            String transaction = ele[1];
            String timestamp = ele[2];
            String user = ele[3];

            req_exe.acquire();
            map.get(ele[0]).set_timestamp(clock_var.toString());
            clock_var++;
            req_exe.release();
            System.out.println("curr_p : "+ ele[0]);

            String value = "";
            if (ele.length == 5)
                value =  ele[4];

            String finalValue = value;
            new Thread() {
                public void run() {


                    try {
                        map.get(ele[0]).exec_req(process_id, transaction, user, finalValue, trans);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }
/*
            if (ele[0].equals("P1")) {
               // System.out.println(ele[1] + " : "+ele[2] + " : "+ele[3] + "\n");
//                new ThreadHandler(p1, process_id, timestamp, transaction, user, value).start();
                new Thread() {
                    public void run() {


                        try {
                            p1.exec_req(process_id, timestamp, transaction, user, value);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }.start();

            } else if (ele[0].equals("P2")) {
                //System.out.println(ele[1] + ele[2] + ele[3] + "\n");
                //new ThreadHandler(p2, process_id, timestamp, transaction, user, value).start();
                new Thread() {
                    public void run() {

                        try {
                            p2.exec_req(process_id, timestamp, transaction, user, value);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                }.start();

            } else if (ele[0].equals("P3")) {
               // System.out.println(ele[1] + ele[2] + ele[3] + "\n");
              //  new ThreadHandler(p3, process_id, timestamp, transaction, user, value).start();
                new Thread() {
                    public void run() {

                        try {
                            p3.exec_req(process_id, timestamp, transaction, user, value);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                    }
                }.start();

            } else {
                // invalid prcess id
            }

        }
        */
        //  Thread.sleep(2000); /*MAINTAIN THE CLOCK CYCLE*/
        //ThreadHandler myThreads[] = new ThreadHandler[processes];

    }


}

