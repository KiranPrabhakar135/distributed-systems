package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.res.ColorStateList;
import android.graphics.Color;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static final List<String> ports = Arrays.asList(REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4);
    static final String SOCKET_CLOSE_ACK = "communication ended";
    static final String PROPOSAL_RESPONSE = "Response to proposal";
    static final String PROPOSAL_REQUEST = "Request for proposal";
    static final String AGREED_SEQUENCE_MSG = "Agreed Sequence message";
    static final String PROCESS_FAILURE_MSG = "Process Crashed";
    static final String PROCESS_FAILURE_DETECTED_MSG = "Process Failure Detected";
    static final String PING_MSG = "Are you alive?";
    private static long sqliteKey = 0;
    private long proposal = 0;
    private long messageId = 0;
    private static final Object sqliteKeySyncObject = new Object();
    private static final Object proposalObject = new Object();
    private static final Object processProposalSyncObject = new Object();
    private static final Object messagesSentSyncObject = new Object();
    private static final Object priorityQueueSyncObj = new Object();
    private static final Object fifoSyncObj = new Object();
    private static final Object failedPortObj = new Object();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static String myProcessId ="";
    private static String failedProcessPort ="";
    private float[] proposals = new float[5];
    // Ensures the total ordering
    private PriorityQueue<QueueObject> priorityQueue;
    // To store the proposals from all the processes for a particular message.
    private HashMap<String, List<Float>> proposalsMap;
    private HashMap<String, List<String>> proposedProcess;
    private int socketTimeoutInMilliSeconds = 3000;
    private static boolean isFirstMessage = true;

    // Ensures FIFO ordering
    private HashMap<String, Integer> fifoMap;
    private HashMap<String, Queue<QueueObject>> fifoQueueMap = new HashMap<String, Queue<QueueObject>>();

    private String getFailedProcessesPort(){
        synchronized (failedPortObj){
            //Log.i("getFailedPort", "The failed port is: " + failedProcessPort);
            return failedProcessPort;
        }
    }

    private void setProcessFailedPort(String failedPort){
        synchronized (failedPortObj){
            if(failedProcessPort.isEmpty()){
                failedProcessPort = failedPort;
            }
        }
    }

    private final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private void incrementKey(){
        synchronized (sqliteKeySyncObject){
            sqliteKey++;
        }
    }

    private void incrementOrResetProposal(int newValue){
        synchronized (proposalObject){
            if(newValue == -1){
                proposal++;
            }
            else{
                if(newValue <= proposal){
                    Log.i("Inc Proposal", "New value is " + newValue + " proposal is: " + proposal);
                    proposal++;
                }
                else{
                    proposal = newValue;
                }
            }
        }
    }

    private void incrementMessageId(){
        synchronized (messagesSentSyncObject){
            messageId++;
        }
    }
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        final String myPort = getMyPort();

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        priorityQueue = new PriorityQueue<QueueObject>(11, new PriorityQueueComparator());
        fifoMap = new HashMap<String, Integer>();
        for (String port : ports) {
            fifoMap.put(port,0);
            fifoQueueMap.put(port, new LinkedList<QueueObject>());
        }

        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e("On create", "Can't create a ServerSocket");
            return;
        }
        proposalsMap = new HashMap<String, List<Float>>();
        proposedProcess= new HashMap<String, List<String>>();
        final EditText editText = (EditText) findViewById(R.id.editText1);
        Button sendButton = (Button) findViewById(R.id.button4);
        sendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(" ");
                tv.setTextColor(ColorStateList.valueOf(Color.rgb(65, 65, 244)));
                tv.append("\t sent message is: " + msg);
                msg = PROPOSAL_REQUEST + "-" + msg;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });

    }

    private String getMyPort() {
        if(! myProcessId.isEmpty()){
            return myProcessId;
        }
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myProcessId =  String.valueOf((Integer.parseInt(portStr) * 2));
        return myProcessId;
    }

    private class QueueObject{
        int messageId;
        String requestedProcessId;
        float sequenceNumber;
        boolean isDeliverable;
        String message;

        @Override
        public String toString() {
            return requestedProcessId + ":" + messageId + "-" + sequenceNumber + ":" + isDeliverable + ":" + message;
        }
    }

    private class PriorityQueueComparator implements Comparator<QueueObject>{
        @Override
        public int compare(QueueObject lhs, QueueObject rhs) {
            return Float.compare(lhs.sequenceNumber, rhs.sequenceNumber);
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private final ContentResolver mContentResolver = getContentResolver();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.i("In server","message received on socket - " + serverSocket.getLocalPort());
            Socket socket = null;

            try {
                while(true){
                    socket =  serverSocket.accept();
                    BufferedReader br = new BufferedReader( new InputStreamReader(socket.getInputStream()));
                    String msg = br.readLine();

                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    PrintWriter pw = new PrintWriter(dataOutputStream, true);
                    pw.println(SOCKET_CLOSE_ACK);
                    Log.i("In server","Sent acknowledgement and received msg is " + msg);
                    String[] msgContents = msg.split("-");
                    String msgType = msgContents[0];

                    if(msgType.equals(PROPOSAL_RESPONSE)){
                        String receivedFrom = msgContents[1];
                        String msgId = msgContents[2];
                        String proposal = msgContents[3];
                        Log.i("InServ-PROP_RES", "Received the proposal message for msg Id "+ msgId + " and the value is " + proposal + "- received from " + receivedFrom);
                        processProposalResponses(msgId, proposal, receivedFrom);
                    }
                    else if(msgType.equals(AGREED_SEQUENCE_MSG)){
                        String remoteProcessId = msgContents[1];
                        String messageId = msgContents[2];
                        String agreedSequence = msgContents[3];
                        Log.i("InServ-AGR_RES", "Agreed Seq for " + messageId + " is " + agreedSequence);
                        updatePriorityQueue(Integer.parseInt(messageId), Float.parseFloat(agreedSequence), remoteProcessId);
                    }
                    else if(getFailedProcessesPort().isEmpty() &&(msgType.equals(PROCESS_FAILURE_MSG) || msgType.equals(PROCESS_FAILURE_DETECTED_MSG))){
                        String failedProcess = msgContents[1];
                        setProcessFailedPort(failedProcess);
                        if(msgType.equals(PROCESS_FAILURE_DETECTED_MSG)){
                            sendMessageThroughAsyncTask(PROCESS_FAILURE_MSG + "-" + failedProcess);
                        }
                        removeFailedProcessState(failedProcess);
                    }
                    else if(msgType.equals(PROPOSAL_REQUEST)){
                        String remotePort = msgContents[1];
                        String localPort = msgContents[2];
                        String messageId = msgContents[3];
                        //Log.i("In server","The local port is "+ localPort +" the remote port is: " + remotePort);
                        processProposalRequest(msgContents, localPort, remotePort, messageId, 4);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e){
                e.printStackTrace();
            } finally {
                try {
                    Log.i("In Server:", "Closing the socket");
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }

        private void removeFailedProcessState(String processId){
            Log.i("Cleaning State", "removing failed process state of process: "+ processId);
            synchronized (priorityQueueSyncObj){
                Iterator<QueueObject> iterator = priorityQueue.iterator();
                while (iterator.hasNext()){
                    QueueObject next = iterator.next();
                    if(next.requestedProcessId.equals(processId)){
                        priorityQueue.remove(next);
                    }
                }
                QueueObject[] queueObjects = new QueueObject[priorityQueue.size()];
                queueObjects = priorityQueue.toArray(queueObjects);
                Arrays.sort(queueObjects,new PriorityQueueComparator());
                priorityQueue.clear();
                Collections.addAll(priorityQueue, queueObjects);
                Log.i("Cleaning State", "Cleaned Priority Queue: "+ processId);
            }
            synchronized (processProposalSyncObject) {
                if(proposalsMap.size() > 0){
                    List<String> keysTobeRemoved = new ArrayList<String>();
                    for (String msgId : proposalsMap.keySet()) {
                        if(proposedProcess.containsKey(msgId)){
                            List<String> proposedProcesses = proposedProcess.get(msgId);
                            if(proposedProcesses.size() == ports.size() -1
                                && !proposedProcesses.contains(getFailedProcessesPort())
                                && proposalsMap.get(msgId).size() == ports.size()-1){
                                decideAndPublishAgreedSequence(msgId,proposalsMap.get(msgId));
                                keysTobeRemoved.add(msgId);
                            }
                        }
                    }
                    for (String key : keysTobeRemoved) {
                        proposalsMap.remove(key);
                        proposedProcess.remove(key);
                    }
                }
                Log.i("Cleaning State", "Cleaned Proposals Map and ProposedProcesses: "+ processId);
            }
        }

        private void processProposalRequest(String[] msgContents, String localPort, String remotePort, String messageId, int msgStartIndex) {
            Integer msgId = Integer.parseInt(messageId);
            QueueObject queueObject = new QueueObject();
            queueObject.messageId = msgId;
            queueObject.message = getRawMessage(msgContents,msgStartIndex);
            queueObject.isDeliverable = false;
            queueObject.requestedProcessId = remotePort;

            synchronized (fifoSyncObj){
                if(getFailedProcessesPort().equals(remotePort)){
                    fifoMap.remove(remotePort);
                    fifoQueueMap.remove(remotePort);
                }
                if(fifoMap.containsKey(remotePort)){
                    int existingMessageId = fifoMap.get(remotePort);
                    if(existingMessageId + 1 == msgId){
                        List<QueueObject> msgsReadyToGetProposals =  new ArrayList<QueueObject>();
                        msgsReadyToGetProposals.add(queueObject);
                        if(fifoQueueMap.containsKey(remotePort) && fifoQueueMap.get(remotePort).size() > 0){
                            Queue<QueueObject> queue = fifoQueueMap.get(remotePort);
                            int currentId = msgId;
                            while (currentId + 1 == queue.peek().messageId){
                                QueueObject objectReadyForProposal = queue.poll();
                                msgsReadyToGetProposals.add(objectReadyForProposal);
                                currentId = objectReadyForProposal.messageId;
                                Log.i("fifo", "got message in fifo order");
                            }
                        }
                        for (QueueObject obj : msgsReadyToGetProposals) {
                            proposePriority(localPort, remotePort, obj);
                            fifoMap.put(remotePort, obj.messageId);
                        }
                    }
                    else{
                        if(fifoQueueMap.containsKey(remotePort)){
                            fifoQueueMap.get(remotePort).add(queueObject);
                        }
                    }
                }
            }
        }

        private void proposePriority(String localPort, String remotePort, QueueObject queueObject) {
            synchronized (priorityQueueSyncObj) {
                incrementOrResetProposal(-1);
                queueObject.sequenceNumber = proposal + convertPortToDecimal(localPort);// Store it in priority queue
                priorityQueue.add(queueObject);
                String proposalMsg = PROPOSAL_RESPONSE + "-" + queueObject.messageId + "-" + queueObject.sequenceNumber + "-" + remotePort;
                sendMessageThroughAsyncTask(proposalMsg);
            }
        }

        private synchronized void processProposalResponses(String msgId, String proposal, String receivedFrom) {
            synchronized (processProposalSyncObject){
                if(!proposalsMap.containsKey(msgId)){
                    proposalsMap.put(msgId, new ArrayList<Float>());
                }
                if(!proposedProcess.containsKey(msgId)){
                    proposedProcess.put(msgId, new ArrayList<String>());
                }
                if(proposalsMap.containsKey(msgId) && proposedProcess.containsKey(msgId)){
                    List<Float> values = proposalsMap.get(msgId);
                    values.add(Float.parseFloat(proposal));
                    List<String> processes = proposedProcess.get(msgId);
                    processes.add(receivedFrom);
                    boolean hasAllResponsesReceived = ((values.size() == ports.size()) ||
                            (!getFailedProcessesPort().isEmpty() && !processes.contains(getFailedProcessesPort())
                                &&  values.size() == ports.size()-1));
                    Log.i("check if all received", "The count of proposals map is: " + values.size() + " The value of failed Port: " + getFailedProcessesPort());

                    if(hasAllResponsesReceived){
                        decideAndPublishAgreedSequence(msgId, values);
                        proposalsMap.remove(msgId);
                        proposedProcess.remove(msgId);
                    }
                }
            }
        }

        private void decideAndPublishAgreedSequence(String msgId, List<Float> values) {
            Float agreedProposal = Collections.max(values);
            String agreedProposalMessage = AGREED_SEQUENCE_MSG + "-" + msgId + "-" + agreedProposal;
            int newProposalValue = (int)Math.ceil(agreedProposal);
            incrementOrResetProposal(newProposalValue);
            sendMessageThroughAsyncTask(agreedProposalMessage);
            Log.i("agreedProp-process", "agreedProp-process " + agreedProposal);
        }

        private void updatePriorityQueue(int messageId, float newSequenceNumber, String processId){
            synchronized (priorityQueueSyncObj){
                List<QueueObject> polledObjects = new ArrayList<QueueObject>();
                while (!priorityQueue.isEmpty()){
                    QueueObject next = priorityQueue.poll();
                    // This is the case where the failure message comes first and the request for
                    // proposal comes from the failed process later
                    if(next.requestedProcessId.equals(failedProcessPort)){
                        Log.i("updatePriQueue", "The failed resource is not still cleaned " + next.toString());
                        continue;
                    }
                    if(next.messageId == messageId && next.requestedProcessId.equals(processId)){
                        Log.i("updatePriQueue", "set deliverable for message " + messageId
                                + " from process " + processId+ " the length is: "
                                + priorityQueue.size());
                        next.sequenceNumber = newSequenceNumber;
                        next.isDeliverable = true;
                        priorityQueue.add(next);
                        if(polledObjects.size() > 0){
                            priorityQueue.addAll(polledObjects);
                        }
                        break;
                    }
                    else{
                        polledObjects.add(next);
                    }
                    if(priorityQueue.isEmpty() && polledObjects.size() > 0){
                        priorityQueue.addAll(polledObjects);
                        break;
                    }
                }
                Log.i("test","Entering next while loop " + priorityQueue.isEmpty());

                // Sort the priority Queue. This looks redundant to me. Priority queue should implicitly do this..
                QueueObject[] queueObjects = new QueueObject[priorityQueue.size()];
                queueObjects = priorityQueue.toArray(queueObjects);
                Arrays.sort(queueObjects,new PriorityQueueComparator());
                priorityQueue.clear();
                Collections.addAll(priorityQueue, queueObjects);

                while (!priorityQueue.isEmpty()){
                    QueueObject nextObject = priorityQueue.peek();
                    if(nextObject.requestedProcessId.equals(getFailedProcessesPort())){
                        // This is the case where the failure message comes first and the request for
                        // proposal comes from the failed process later
                        Log.i("updatePriQueue", "The failed resource is not still cleaned " + nextObject.toString());
                        priorityQueue.poll();
                        continue;
                    }
                    if(nextObject.isDeliverable){
                        Log.i("Deliver", "Delivering message " + nextObject.messageId +
                                " from process " + nextObject.requestedProcessId+
                                " the length is: " + priorityQueue.size());
                        ContentValues contentValues = new ContentValues();
                        contentValues.put(KEY_FIELD, Long.toString(sqliteKey));
                        contentValues.put(VALUE_FIELD, nextObject.message);
                        mContentResolver.insert(uri, contentValues);
                        incrementKey();
                        QueueObject deliveredObj = priorityQueue.poll();
                        Log.i("Delivered Object:", deliveredObj.toString());
                        publishProgress(nextObject.message);
                    }
                    else{
                        Log.i("Not Deliverable", nextObject.messageId + " from process "
                                + nextObject.requestedProcessId + " the length is: "
                                + priorityQueue.size());
                        break;
                    }
                }
            }
        }

        private String getRawMessage(String[] contents, int index){
            StringBuilder stringBuilder= new StringBuilder();
            for (int i= index; i< contents.length; i++){
                stringBuilder.append(contents[i]);
            }
            return stringBuilder.toString();
        }

        void sendMessageThroughAsyncTask(String msg){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }

        private float convertPortToDecimal(String port){
            return Float.parseFloat(port)/100000;
        }

        protected void onProgressUpdate(String...strings) {
            String msg = strings[0].trim() + "\n";
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.setTextColor(ColorStateList.valueOf(Color.rgb(244, 65, 103)));
            tv.append("\t received message is: " + msg);

            String strReceived = strings[0].trim();
            String filename = "SimpleMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e("On", "File write failed");
            }
            return;
        }
    }

    private class PingTask extends AsyncTask<String, String, Void>{
        @Override
        protected Void doInBackground(String... serverSockets) {
            try {
                // Introducing delay to allow all processes to come up.
                Thread.sleep(7000);
                while(true){
                    boolean failureDetected = false;
                    for (String port: ports) {
                        try {
                            if(getFailedProcessesPort().isEmpty()){
                                // Log.i("Ping" ,"Started to ping the port: " + port);
                                Socket socket = getSocket(port);

                                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                                PrintWriter pw = new PrintWriter(dataOutputStream, true);
                                pw.println(PING_MSG);
                                closeClientSocketOnReceivingAck(socket, pw);
                            }
                            else{
                                failureDetected = true;
                                break;
                            }
                        } catch (SocketTimeoutException e) {
                            Log.e("Error in ping","SocketTimeoutException in ping task on port: " +port);
                            failureDetected = true;
                            intimateMyServerUponProcessFailure(port);
                            break;
                        } catch (SocketException e) {
                            Log.e("Error in ping","SocketException in ping task  on  port: " +port);
                            failureDetected = true;
                            intimateMyServerUponProcessFailure(port);
                            break;
                        } catch (IOException e){
                            Log.e("Error in ping", "IO exceptions in Ping task failed in ping task  on port: " + port);
                            failureDetected = true;
                            intimateMyServerUponProcessFailure(port);
                            break;
                        }catch (Exception e){
                            Log.e("exc", "all exceptions");
                            failureDetected = true;
                            break;
                        }
                    }
                    Thread.sleep(3000);
                    if(failureDetected){
                        break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Log.i("Inside Ping task", "Ending ping task ");
            return null;
        }
    }

    private void closeClientSocketOnReceivingAck(Socket socket, PrintWriter pw) throws IOException {
        socket.setSoTimeout(socketTimeoutInMilliSeconds);

        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String msg = br.readLine();

        if (msg.equals(SOCKET_CLOSE_ACK)) {
            Log.i("close", "received ack");
            pw.flush();
            pw.close();
            socket.close();
        }
    }

    private void intimateMyServerUponProcessFailure(String failedPort){
        if(getFailedProcessesPort().isEmpty()){
            Log.i("Failure Detected", "Failed to connect to port "+ failedPort);
            try {
                Socket socket = getSocket(getMyPort());
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                PrintWriter pw = new PrintWriter(dataOutputStream, true);
                pw.println(PROCESS_FAILURE_DETECTED_MSG + "-" + failedPort);
                closeClientSocketOnReceivingAck(socket, pw);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Socket getSocket(String port) throws IOException {
        return new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(port));
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected void onPreExecute() {
            if(isFirstMessage){
                isFirstMessage = false;
                new PingTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"");
            }
        }

        @Override
        protected synchronized Void doInBackground(String... msgs) {
            try {
                String[] messageContent = msgs[0].split("-");
                String messageType = messageContent[0];
                Log.i("In client", "Reached Client task ");
                if(messageType.equals(PROPOSAL_RESPONSE)){
                    String toProcess = "";
                    try {
                        String messageId = messageContent[1];
                        String proposedSequence = messageContent[2];
                        toProcess = messageContent[3];
                        if(toProcess.equals(getFailedProcessesPort())){
                            return null;
                        }
                        Log.i("PropRes-Client","Received proposal message is: " + msgs[0]);
                        Socket socket = getSocket(toProcess);

                        Log.i("PropRes-Client","Received proposal from server for message " + messageId +
                                " Sending it to process: "+ toProcess);

                        String proposalRequestMsg = PROPOSAL_RESPONSE + "-"+ getMyPort() +"-" + messageId + "-" + proposedSequence;
                        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                        PrintWriter pw = new PrintWriter(dataOutputStream, true);
                        pw.println(proposalRequestMsg);
                        closeClientSocketOnReceivingAck(socket, pw);
                    } catch (SocketTimeoutException e) {
                        Log.e("Err in client PropResp","PropResp: SocketTimeoutException on port: " +toProcess);
                        intimateMyServerUponProcessFailure(toProcess);
                    } catch (SocketException e) {
                        Log.e("Err in client PropResp","PropResp: SocketException on port: " +toProcess);
                        intimateMyServerUponProcessFailure(toProcess);
                    } catch (IOException e){
                        Log.e("Err in client PropResp", "PropResp: IO exceptions");
                        intimateMyServerUponProcessFailure(toProcess);
                    }catch (Exception e){
                        Log.e("Err in client PropResp", "PropResp: all exceptions");
                    }
                }
                else if(messageType.equals(PROPOSAL_REQUEST)
                        || messageType.equals(AGREED_SEQUENCE_MSG)
                        || messageType.equals(PROCESS_FAILURE_MSG)){

                    if(messageType.equals(PROPOSAL_REQUEST)){
                        incrementMessageId();
                    }
                    for (String port: ports) {
                        try {
                            if(port.equals(getFailedProcessesPort())){
                                continue;
                            }
                            if(messageType.equals(PROCESS_FAILURE_MSG) && port.equals(getMyPort())){
                                continue;
                            }
                            Socket socket = getSocket(port);
                            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                            PrintWriter pw = new PrintWriter(dataOutputStream, true);

                            if(messageType.equals(PROPOSAL_REQUEST)){
                                Log.i("PropReq-Client", "sent proposal request to the port: " + port);
                                String msgToSend = messageContent[1];
                                String proposalRequestMsg = PROPOSAL_REQUEST + "-" + getMyPort() +"-"+ port +"-" + messageId + "-"+ msgToSend;
                                pw.println(proposalRequestMsg);
                            }
                            else if(messageType.equals(AGREED_SEQUENCE_MSG)){
                                String messageId = messageContent[1];
                                String agreedProposal = messageContent[2];
                                Log.i("AgrSeq-Client","Received agreed proposal from server for message " + messageId +
                                        " the value is: " + agreedProposal);
                                String proposalRequestMsg = AGREED_SEQUENCE_MSG + "-" + getMyPort() +"-" + messageId + "-" + agreedProposal;
                                pw.println(proposalRequestMsg);
                            }
                            else if(messageType.equals(PROCESS_FAILURE_MSG)){
                                String failedProcess = messageContent[1];
                                Log.i("AgrSeq-Client","Received failed process port as: "+ failedProcess);
                                String failedProcessMsg = PROCESS_FAILURE_MSG + "-" + failedProcess;
                                pw.println(failedProcessMsg);
                            }
                            closeClientSocketOnReceivingAck(socket, pw);
                        } catch (SocketTimeoutException e) {
                            Log.e("Err in client AggSeq","AggSeq SocketTimeoutException on port: " +port);
                            intimateMyServerUponProcessFailure(port);
                        } catch (SocketException e) {
                            e.printStackTrace();
                            Log.e("Err in client AggSeq","AggSeqSocketException on port: " +port);
                            intimateMyServerUponProcessFailure(port);
                        } catch (IOException e){
                            Log.e("Err in client AggSeq", "AggSeq IO exceptions " + port);
                            intimateMyServerUponProcessFailure(port);
                        }catch (NullPointerException e){
                            Log.e("Err in client AggSeq", "AggSeq all null pointer exceptions"+ port);
                        }catch (Exception e){
                            Log.e("Err in client AggSeq", "AggSeq all other exceptions"+ port);
                        }
                    }
                }
            } catch (Exception e) {
                Log.e("Client Task", "ClientTask all Exception");
            }
            return null;
        }
        @Override
        protected void onPostExecute (Void result){
            Log.i("In client","The task is completed..");
        }
    }
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
