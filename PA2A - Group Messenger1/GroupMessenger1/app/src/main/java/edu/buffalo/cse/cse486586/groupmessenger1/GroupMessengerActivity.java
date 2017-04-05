package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Button;
import android.view.View.OnClickListener;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    int counter = 0;


    String[] ports = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */


        //super.onCreate(savedInstanceState);

        /*
         * Allow this Activity to use a layout file that defines what UI elements to use.
         * Please take a look at res/layout/main.xml to see how the UI elements are defined.
         *
         * R is an automatically generated class that contains pointers to statically declared
         * "resources" such as UI elements and strings. For example, R.layout.main refers to the
         * entire UI screen declared in res/layout/main.xml file. You can find other examples of R
         * class variables below.
         */
        //setContentView(R.layout.main);

        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        final EditText editText1 = (EditText) findViewById(R.id.editText1);

        try {

            ServerSocket socket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);

        } catch (Exception e) {
            e.printStackTrace();
        }


        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        final Button button4 = (Button) findViewById(R.id.button4);
        button4.setOnClickListener(new OnClickListener() {
            @Override
            public void onClick(View v) {

                String msg = editText1.getText().toString();
                editText1.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                // This is one way to reset the input box.
                //.append(msg);
                // startActivity(new Intent(GroupMessengerActivity.this, Sta));


            }
        });


    }

    //tv.setOnClickListener();
       /*TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];



            do {


                try {

                    Socket socket1 = serverSocket.accept();
                    //PrintWriter output = new PrintWriter(socket1.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket1.getInputStream()));
                    String input;

                    while ((input = in.readLine()) != null) {


                        //input = "";
                        //Log.e("recieved : ",input);
                        // output.println(input);
                        ContentValues value1 = new ContentValues();
                        value1.put("key", counter);
                        value1.put("value", input.trim());
                       // Log.e("CV values : ", value1.toString());
                        getContentResolver().insert(uri, value1);
                        publishProgress(input);

                        counter++;
                    }

                    in.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            } while (true);
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            //return null;
        }

        protected void onProgressUpdate(String... strings) {

            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            //TextView localTextView = (TextView) findViewById(R.id.textView1);
            //localTextView.append("\n");//lowing code displays what is received in doInBackground().

            //String strReceived = strings[0].trim();
           // EditText editText1 = (EditText) findViewById(R.id.editText1);
            //String msg = editText1.getText().toString();
            //editText1.setText(""); // This is one way to reset the input box.

            //TextView tv = (TextView) findViewById(R.id.textView1);
            //tv.append(msg);

            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {


            for (int i = 0; i < ports.length; i++) {

                try {
                    String remotePort = ports[i];
                    //if (msgs[0].equals(ports[i]))
                    //remotePort = ports[i + 1];

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    String msgToSend = msgs[0];

                    if (socket != null ) {
                        //PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream());
                        Log.e(TAG, "Sent message to clients: "+msgToSend +" ---" +remotePort);
                        out.write(msgToSend);
                        out.flush();
                        out.close();
                    }

                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }

            }
            return null;

        }


        public boolean onCreateOptionsMenu(Menu menu) {
            // Inflate the menu; this adds items to the action bar if it is present.
            getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
            return true;
        }
    }
}