package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import static android.content.ContentUris.withAppendedId;

public class SimpleDhtProvider extends ContentProvider {
    private String node_id;
    private String myPort;
    private DatabaseHelper db;
    private String leaderPort = "11108";
    static final int SERVER_PORT = 10000;
    static final String SOCKET_CLOSE_ACK = "Close the socket";
    static final String NODE_JOIN_REQUEST = "Position me on the ring";
    static final String NODE_JOIN_UPDATE = "Found New Nodes Position in the ring";
    static final String INSERT_REQ = "Insert the Key";
    static final String DELETE_REQ = "Delete Request";
    static final String QUERY_REQ = "Query Request";
    static final String QUERY_RESPONSE = "Query Response";
    static final String RETAIN = "RETAIN";
    static final String REACHED_START_NODE = "Reached Start Node";
    static String QUERIED_VALUE = "";
    static String predecessorPort = "";
    static String predecessorNodeId = "";
    static String successorPort = "";
    static String successorNodeId = "";
    private int socketTimeoutInMilliSeconds = 3000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    private static int deleteCounter;
    private static int queryCounter;
    private static final Object deleteCounterSyncObj = new Object();
    private static final Object queryCounterSyncObj = new Object();

    private void incrementQueryCounter(){
        synchronized (queryCounterSyncObj){
            queryCounter++;
        }
    }

    private int getQueryCounter(){
        synchronized (queryCounterSyncObj){
            return queryCounter;
        }
    }

    private void setQueryCounter(int value){
        synchronized (queryCounterSyncObj){
            queryCounter = value;
        }
    }

    private int getDeleteCounter(){
        synchronized (deleteCounterSyncObj){
            return deleteCounter;
        }
    }

    private void setDeleteCounter(boolean isIncrement, int value){
        synchronized (deleteCounterSyncObj){
            if(isIncrement){
                deleteCounter++;
            }
            else{
                deleteCounter = value;
            }
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(isNumeric(selection) && Integer.valueOf(selection) == getDeleteCounter() || QUERIED_VALUE.equals(selection)){
            Log.i("Delete","Reached the request initiator node..");
            return 0;
        }

        if((selection.equalsIgnoreCase("*")
                || (isNumeric(selection) && Integer.valueOf(selection) > getDeleteCounter())
                || !isNumeric(selection)) && !selection.equalsIgnoreCase("@")){
            Log.i("Delete","(*, key, recurse) selection delete executed. " +selection);
            if(noOtherNodeInRing()){
                if(selection.equalsIgnoreCase("*")){
                    db.deleteAllInDB();
                }
                if(!isNumeric(selection)){
                    db.delete(uri.toString(),selection,selectionArgs);
                }
                return 0;
            }
            try{
                String deleteMessage;
                if(isNumeric(selection)){
                    db.deleteAllInDB();
                    setDeleteCounter(false, Integer.valueOf(selection));
                    deleteMessage = DELETE_REQ + "-" + getDeleteCounter();
                }
                else if(selection.equalsIgnoreCase("*")){
                    setDeleteCounter(true,-1);
                    deleteMessage = DELETE_REQ + "-" + getDeleteCounter();
                }
                else{
                    QUERIED_VALUE = selection;
                    if(isInMyKeySpace(genHash(QUERIED_VALUE))){
                        Log.i("Delete", "Returning the value for key.." + selection);
                        QUERIED_VALUE = "";
                        db.delete(uri.toString(), selection,selectionArgs);
                        return 0;
                    }
                    else{
                        Log.i("Delete", "Routing the Single Delete Request for " + selection);
                        deleteMessage = DELETE_REQ + "-" + QUERIED_VALUE;
                    }
                }
                Socket socket = getSocket(successorPort);
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                PrintWriter pw = new PrintWriter(dataOutputStream, true);
                pw.println(deleteMessage);

                socket.setSoTimeout(socketTimeoutInMilliSeconds);
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msg = br.readLine();
                if(msg.equals(SOCKET_CLOSE_ACK)){
                    pw.flush();
                    pw.close();
                    br.close();
                    socket.close();
                }
            }
            catch (NoSuchAlgorithmException ex){
                ex.printStackTrace();
            }
            catch (IOException ex){
                ex.printStackTrace();
            }
        }
        if(selection.equalsIgnoreCase("@")){
            db.deleteAllInDB();
            return 0;
        }
        else{
            db.delete(uri.toString(),selection,selectionArgs);
            return 0;
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
            String key = values.get(KEY_FIELD).toString();
            String value = values.get(VALUE_FIELD).toString();
            String hashedKey = genHash(key);
            Log.i("Insert", "Received insert key is: " + key + " the hash is: " + hashedKey);
            if(isInMyKeySpace(hashedKey) || noOtherNodeInRing()){
                Log.i("Insert", "Inserting " + key + " hashed key is: " + hashedKey);
                long id = db.insertOrUpdateData(values);
                return withAppendedId(uri, id);
            }
            else{
                // route it
                Log.i("Insert", "Route the insert request for key: " + key);
                String insertMsg = INSERT_REQ + "-" + successorPort + "-" + key + "-" + value;
                sendMessageThroughClientTask(insertMsg);
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            this.node_id = generateNodeId(getMyPort());

            if(db == null){
                db =  new DatabaseHelper(getContext());
            }

            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e("On create", "Can't create a ServerSocket");
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    private MatrixCursor getMatrixCursorFromString(Cursor cursor, MatrixCursor appendTo){
        MatrixCursor response = new MatrixCursor(cursor.getColumnNames());
        Log.i("getMatrixCursor","Cursor length is: " + cursor.getCount());
        if(appendTo != null && appendTo.getCount() > 0){
            appendTo.moveToFirst();
            do{
                response.newRow()
                        .add(KEY_FIELD, appendTo.getString(appendTo.getColumnIndex(KEY_FIELD)))
                        .add(VALUE_FIELD, appendTo.getString(appendTo.getColumnIndex(VALUE_FIELD)));
            }while (appendTo.moveToNext());
        }
        if(cursor.getCount() > 0 ){
            cursor.moveToFirst();
            do{
                response.newRow()
                        .add(KEY_FIELD, cursor.getString(cursor.getColumnIndex(KEY_FIELD)))
                        .add(VALUE_FIELD, cursor.getString(cursor.getColumnIndex(VALUE_FIELD)));
            }while(cursor.moveToNext());
        }
        return response;
    }

    private String getStringFromCursor(Cursor cursor){
        StringBuilder response = new StringBuilder();
        Log.i("getStringFromCursor","Cursor length is: " + cursor.getCount());
        if(cursor.getCount() > 0 ){
            cursor.moveToFirst();
            do{
                response.append(cursor.getString(cursor.getColumnIndex(KEY_FIELD)));
                response.append(":");
                response.append(cursor.getString(cursor.getColumnIndex(VALUE_FIELD)));
                response.append("#");
            }while(cursor.moveToNext());
        }
        return response.toString();
    }

    private MatrixCursor getMatrixCursorFromString(String data){
        MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
        String[] dataItems = data.split("#");
        for (String dataItem : dataItems) {
            Log.i("getCursorFromString","The item is " + dataItem);
            String[] contentValues = dataItem.split(":");
            if(contentValues.length == 2){
                Log.i("getCursorFromString","The Key and Value is " + contentValues[0] + "**" + contentValues[1]);
                cursor.newRow()
                        .add(KEY_FIELD, contentValues[0])
                        .add(VALUE_FIELD, contentValues[1]);
            }
        }
        Log.i("getCursorFromString","The length of cursor returned  is " + cursor.getCount());
        return cursor;
    }

    private boolean isNumeric(String value){
        try{
            Integer.parseInt(value);
            return true;
        }
        catch (NumberFormatException e){
            return false;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        if(isNumeric(selection) && Integer.valueOf(selection) == getQueryCounter() || QUERIED_VALUE.equals(selection)){
            Log.i("Query","Reached the request initiator node..");
            return  null;
        }
        if((selection.equalsIgnoreCase("*")
                || (isNumeric(selection) && Integer.valueOf(selection) > getQueryCounter())
                || !isNumeric(selection)) && !selection.equalsIgnoreCase("@")){
            Log.i("Query","(*, key, recurse) selection query executed. " +selection);
            if(noOtherNodeInRing()){
                if(selection.equalsIgnoreCase("*")){
                    return db.getAllValues();
                }
                if(!isNumeric(selection)){
                    return db.getValue(uri.toString(),projection,selection,selectionArgs,null,null,sortOrder);
                }
            }
            try{
                MatrixCursor cursor = null;
                String queryMsg;
                if(isNumeric(selection)){
                    cursor = getMatrixCursorFromString(db.getAllValues(), null);
                    setQueryCounter(Integer.valueOf(selection));
                    queryMsg = QUERY_REQ + "-" + getQueryCounter();
                }
                else if(selection.equalsIgnoreCase("*")){
                    cursor = getMatrixCursorFromString(db.getAllValues(), null);
                    incrementQueryCounter();
                    queryMsg = QUERY_REQ + "-" + getQueryCounter();
                }
                else{
                    QUERIED_VALUE = selection;
                    if(isInMyKeySpace(genHash(QUERIED_VALUE))){
                        Log.i("Query", "Returning the value for key.." + selection);
                        QUERIED_VALUE = "";
                        return db.getValue(uri.toString(),projection,selection,selectionArgs,null,null,sortOrder);
                    }
                    else{
                        Log.i("Query", "Routing the Single Query Request for " + selection);
                        queryMsg = QUERY_REQ + "-" + QUERIED_VALUE;
                    }
                }

                Socket socket = getSocket(successorPort);
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                PrintWriter pw = new PrintWriter(dataOutputStream, true);
                pw.println(queryMsg);
                socket.setSoTimeout(socketTimeoutInMilliSeconds);
                Log.i("Query", "Started While loop..");
                while(true){
                    BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String msg = br.readLine();
                    if(msg == null || msg.isEmpty()){
                        continue;
                    }
                    Log.i("While loop.", "Received response from server in while loop: " + msg);
                    String[] msgContents = msg.split("-");

                    if(msgContents[0].equals(QUERY_RESPONSE)){
                        Log.i("While loop.", "Got query response from server.. ");
                        MatrixCursor response;
                        String results = msgContents.length < 2 ? "" : msgContents[1];
                        Log.i("While loop.", "The response string from the server is.. " + results);
                        if(results.equals(REACHED_START_NODE)){
                            Log.i("While loop.", "Reached start node and sending empty response.. ");
                            response = getMatrixCursorFromString("");
                        }
                        else{
                            Log.i("While loop.", "Processing the string response from server.. ");
                            response = getMatrixCursorFromString(results);
                        }
                        cursor = getMatrixCursorFromString(response, cursor);
                        Log.i("While loop.", "Closing the socket.. ");
                        pw.flush();
                        pw.close();
                        br.close();
                        socket.close();
                        return cursor;
                    }
                    else{
                        Log.i("While loop.", "Server gave a wrong response breaking the while loop.." + msg);
                        break;
                    }
                }
            }
            catch (NoSuchAlgorithmException ex){
                ex.printStackTrace();
            }
            catch (IOException ex){
                ex.printStackTrace();
            }
        }
        if(selection.equalsIgnoreCase("@")){
            Log.i("Query","@ selection query executed.");
            return db.getAllValues();
        }
        else{
            return db.getValue(uri.toString(),projection,selection,selectionArgs,null,null,sortOrder);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private boolean isInMyKeySpace(String key){
        Log.i("Compare", key + "-" + predecessorNodeId + "-" + successorNodeId);
        if(key.compareTo(node_id) < 0 && key.compareTo(predecessorNodeId) > 0){
            Log.i("Compare", "Key "+ key + " is in my key space..");
            return true;
        }
        if((predecessorNodeId.compareTo(node_id)) > 0 && ((key.compareTo(node_id)) < 0
                || (key.compareTo(predecessorNodeId) > 0 ))){
            Log.i("Compare", "Key "+ key + " is in my key space..");
            return  true;
        }
        Log.i("Compare", "Key not in my key space.. Route the request to successor.");
        return false;
    }

    private boolean amITheInitiator(){
        return getMyPort().equals(leaderPort);
    }

    private boolean noOtherNodeInRing(){
        return (predecessorPort.isEmpty() && successorPort.isEmpty()) ||
                (predecessorPort.equals(leaderPort) && successorPort.equals(leaderPort) && amITheInitiator());
    }

    private String generateNodeId(String port) throws NoSuchAlgorithmException{
        return genHash(Integer.toString(Integer.valueOf(port) / 2));
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String getMyPort() {
        if(myPort != null && !myPort.isEmpty()){
            return myPort;
        }
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort =  String.valueOf((Integer.parseInt(portStr)) * 2);
        return myPort;
    }

    private void sendMessageThroughClientTask(String msg){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
    }

    private Socket getSocket(String port) throws IOException {
        return new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(port));
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        private void processServerResponse(Socket socket, PrintWriter pw, String requesterPort) throws IOException {
            socket.setSoTimeout(socketTimeoutInMilliSeconds);
            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String msg = br.readLine();
            String[] msgContents = msg.split("-");

            if (msgContents[0].equals(SOCKET_CLOSE_ACK) || requesterPort.isEmpty()) {
                Log.i("Client", "received close Ack");
                pw.flush();
                pw.close();
                socket.close();
            }

            if(msgContents[0].equals(NODE_JOIN_UPDATE)){
                try {
                    Log.i("Client", "received Node Join Update Ack");
                    pw.flush();
                    pw.close();
                    socket.close();

                    // If the response arrives to requester itself
                    if(requesterPort.equals(getMyPort())){
                        String acceptedNodePredecessor = msgContents[1];
                        String acceptedNodeSuccessor = msgContents[2];
                        Log.i("Client", "Requester received node join update ");
                        predecessorPort = acceptedNodePredecessor;
                        predecessorNodeId = generateNodeId(predecessorPort);
                        successorPort = Integer.toString(socket.getPort());
                        successorNodeId = generateNodeId(successorPort);
                        sendUpdate(predecessorPort, RETAIN, requesterPort);
                        sendUpdate(successorPort, requesterPort, RETAIN);
                    }
                    else{ // If the request is routed, else part is executed
                        // Send msg to requester
                        sendUpdate(requesterPort, getMyPort(), successorPort);
                        Log.i("Client", "Sent update message to requester.");
                        // Send msg to your Predecessor
                        sendUpdate(successorPort, requesterPort, RETAIN);
                        Log.i("Client", "Sent update message to predecessor.");
                        // Update your predecessor
                        successorPort = requesterPort;
                        successorNodeId = generateNodeId(successorPort);
                        Log.i("Client", "Updated  successorPort Port.");
                    }
                    Log.i("NodeJoinUpdate client", "Node Id: " + getMyPort() + " Predecessor " + predecessorPort + " Successor " + successorPort);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }

        private void sendUpdate(String port, String predecessor, String successor) throws IOException {
            Socket socket = getSocket(port);
            String updateMsg = NODE_JOIN_UPDATE + "-" + predecessor + "-" + successor;
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            PrintWriter pw = new PrintWriter(dataOutputStream, true);
            pw.println(updateMsg);
            processServerResponse(socket, pw, "");
        }

        private void sendDeleteRequest(String successorPort, String message) throws IOException{
            Socket socket = getSocket(successorPort);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            PrintWriter pw = new PrintWriter(dataOutputStream, true);
            pw.println(message);
            processServerResponse(socket, pw, "");
        }

        @Override
        protected synchronized Void doInBackground(String... msgs) {
            try {
                String[] messageContent = msgs[0].split("-");
                String msgType = messageContent[0];

                if(msgType.equals(NODE_JOIN_REQUEST)){
                    try{
                        Log.i("In Client", "received node join req: " + msgs[0]);
                        String sendRequestTo = messageContent[1];
                        Socket socket = getSocket(sendRequestTo);
                        String requesterPort = messageContent[2];
                        String requesterNodeId = messageContent[3];
                        String reqMsg = NODE_JOIN_REQUEST + "-" + requesterPort + "-" + requesterNodeId;
                        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                        PrintWriter pw = new PrintWriter(dataOutputStream, true);
                        pw.println(reqMsg);
                        processServerResponse(socket, pw, requesterPort);
                    }catch (SocketTimeoutException e) {
                        Log.e("Err in Node join req","PropResp: SocketTimeoutException on port: " + messageContent[1]);
                        e.printStackTrace();
                        Thread.sleep(1500);
                    } catch (SocketException e) {
                        Log.e("Err in Node join req","PropResp: SocketException on port: "+ messageContent[1]);
                        e.printStackTrace();
                    } catch (IOException e){
                        Log.e("Err in Node join req", "PropResp: IO exceptions");
                        e.printStackTrace();
                    }catch (Exception e){
                        Log.e("Err in Node join req", "PropResp: all exceptions");
                        e.printStackTrace();
                    }
                }

                if(msgType.equals(INSERT_REQ)){
                    String sendReqTo = messageContent[1];
                    String insertMg = INSERT_REQ + "-" + messageContent[2] + "-" + messageContent[3];
                    Socket socket = getSocket(sendReqTo);
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    PrintWriter pw = new PrintWriter(dataOutputStream, true);
                    pw.println(insertMg);
                    processServerResponse(socket, pw, "");
                }
                if(msgType.equals(DELETE_REQ)){
                    sendDeleteRequest(successorPort, msgs[0]);
                }
            }
            catch (Exception e){
                Log.e("Err in client PropResp", "PropResp: all exceptions");
            }
            return null;
        }
        @Override
        protected void onPostExecute (Void result){
            Log.i("In client","The task is completed..");
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected void onPreExecute(){
            if(!leaderPort.equals(getMyPort())){
                sendNodeJoinRequestMsg(leaderPort, getMyPort(), node_id);
                Log.i("In server","Sent node join request to port - " + leaderPort);
            }
            else{
                predecessorPort = successorPort = leaderPort;
                predecessorNodeId = successorNodeId = node_id;
            }
        }
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.i("In server","Server started");
            ServerSocket serverSocket = sockets[0];
            Log.i("In server","message received by server - " + serverSocket.getLocalPort());
            Socket socket = null;

            try {
                while(true){
                    socket =  serverSocket.accept();
                    BufferedReader br = new BufferedReader( new InputStreamReader(socket.getInputStream()));
                    String msg = br.readLine();

                    Log.i("In server","Received msg at server is " + msg);
                    String[] msgContents = msg.split("-");
                    String msgType = msgContents[0];

                    if(msgType.equals(NODE_JOIN_REQUEST)){
                        Log.i("In server","Received Node join request message ");
                        String requesterPort = msgContents[1];
                        String requesterNodeId = msgContents[2];

                        if(isInMyKeySpace(requesterNodeId) || noOtherNodeInRing()){
                            Log.i("In server","Requester node - " + requesterPort + " is in my key space.");
                            String nodeJoinUpdateMsg = NODE_JOIN_UPDATE + "-" + predecessorPort + "-" + successorPort;
                            respondToClient(socket,nodeJoinUpdateMsg);
                        }
                        else{
                            respondToClient(socket, SOCKET_CLOSE_ACK);
                            sendNodeJoinRequestMsg(successorPort, requesterPort, requesterNodeId);
                            Log.i("In server","Sent node join request to successor port - " + successorPort);
                        }
                    }

                    if(msgType.equals(NODE_JOIN_UPDATE)){
                        String predecessor = msgContents[1];
                        String successor = msgContents[2];
                        predecessorPort = !predecessor.equals(RETAIN) ? predecessor : predecessorPort;
                        successorPort = !successor.equals(RETAIN)? successor :successorPort;
                        predecessorNodeId = generateNodeId(predecessorPort);
                        successorNodeId = generateNodeId(successorPort);
                        Log.i("Node join Update", "Node Id: " + getMyPort() + " Predecessor " + predecessorPort + " Successor " + successorPort);
                        respondToClient(socket, SOCKET_CLOSE_ACK);
                    }

                    if(msgType.equals(INSERT_REQ)){
                        ContentValues values = new ContentValues();
                        values.put(KEY_FIELD, msgContents[1]);
                        values.put(VALUE_FIELD, msgContents[2]);
                        insert(mUri,values);
                        respondToClient(socket, SOCKET_CLOSE_ACK);
                    }
                    if(msgType.equals(DELETE_REQ)){
                        String selection = msgContents[1];
                        delete(mUri, selection, null);
                        respondToClient(socket, SOCKET_CLOSE_ACK);
                    }
                    if(msgType.equals(QUERY_REQ)){
                        Log.i("Server", "Got the request for key " + msgContents[1]);
                        Cursor cursor = query(mUri, null, msgContents[1], null, null);
                        String response = cursor == null ? REACHED_START_NODE : getStringFromCursor(cursor);
                        respondToClient(socket, QUERY_RESPONSE + "-" + response);
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

        private void respondToClient(Socket socket, String msg) throws IOException {
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            PrintWriter pw = new PrintWriter(dataOutputStream, true);
            pw.println(msg);
        }

        private void sendNodeJoinRequestMsg(String requestTo, String requesterPort, String requesterNodeId){
            String nodeJoinMsg =  NODE_JOIN_REQUEST + "-" + requestTo + "-" + requesterPort + "-" + requesterNodeId;
            sendMessageThroughClientTask(nodeJoinMsg);
        }


        protected void onProgressUpdate(String...strings) {

            return;
        }
    }
}