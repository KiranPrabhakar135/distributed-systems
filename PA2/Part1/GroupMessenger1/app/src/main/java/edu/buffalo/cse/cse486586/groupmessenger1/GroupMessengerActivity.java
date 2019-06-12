package edu.buffalo.cse.cse486586.groupmessenger1;

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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

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
    static final String SERVER_ACK = "received";
    private static long key = 0;
    private static Object syncObject = new Object();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private void incrementKey(){
        synchronized (syncObject){
            key++;
        }
    }
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
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
        final EditText editText = (EditText) findViewById(R.id.editText1);
        Button sendButton = (Button) findViewById(R.id.button4);
        sendButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(" ");
                tv.setTextColor(ColorStateList.valueOf(Color.rgb(65, 65, 244)));
                tv.append("\t sent message is: " + msg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private final ContentResolver mContentResolver = getContentResolver();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Log.i("In server","message received on socket - " + serverSocket.getLocalPort());

            try {
                while(true){
                    Socket socket = serverSocket.accept();
                    DataInputStream dataInputStream = new DataInputStream(
                            new BufferedInputStream(socket.getInputStream()));
                    String msg = dataInputStream.readUTF();
                    ContentValues contentValues = new ContentValues();
                    contentValues.put(KEY_FIELD, Long.toString(key));
                    contentValues.put(VALUE_FIELD, msg);
                    Log.i("In server","The received message is: " + msg);
                    mContentResolver.insert(uri, contentValues);
                    incrementKey();
                    publishProgress(msg);

                    // server sends acknowledgement to client to close the socket
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(SERVER_ACK);
                    Log.i("In server","Sent acknowledgement message");
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e){
                e.printStackTrace();
            }
            return null;
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


    private class ClientTask extends AsyncTask<String, Void, Void> {
        private final ContentResolver mContentResolver = getContentResolver();
        @Override
        protected synchronized Void doInBackground(String... msgs) {
            try {
                Log.i("In client","Message to send is: " + msgs[0]);
                for (String port: ports) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    String msgToSend = msgs[0];
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(msgToSend);

                    // Receive the acknowledgement message from server to close the socket
                    DataInputStream dataInputStream = new DataInputStream(
                            new BufferedInputStream(socket.getInputStream()));
                    String ackMsg = dataInputStream.readUTF();

                    Log.i("In client","closing the socket with port number " + port + " acknowledgement is " + ackMsg);
                    if (ackMsg.equals(SERVER_ACK)){
                        socket.close();
                        dataInputStream.close();
                        dataOutputStream.close();
                        Log.i("In client","The socket is closed.. " + port);
                    }
                }
            } catch (UnknownHostException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask socket IOException");
            }
            return null;
        }
        @Override
        protected void onPostExecute (Void result){
            Log.i("In client","The task is completed..");
        }
    }
}
