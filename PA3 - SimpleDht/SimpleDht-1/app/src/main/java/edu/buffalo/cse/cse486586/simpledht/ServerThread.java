/*
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

*/
/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *//*

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

        */
/*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         *//*



        //super.onCreate(savedInstanceState);

        */
/*
         * Allow this Activity to use a layout file that defines what UI elements to use.
         * Please take a look at res/layout/main.xml to see how the UI elements are defined.
         *
         * R is an automatically generated class that contains pointers to statically declared
         * "resources" such as UI elements and strings. For example, R.layout.main refers to the
         * entire UI screen declared in res/layout/main.xml file. You can find other examples of R
         * class variables below.
         *//*

        //setContentView(R.layout.main);

        */
/*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         *//*

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


        */
/*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         *//*

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
       */
/*TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         *//*



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
            */
/*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             *//*

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

    */
/***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *//*

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
package edu.buffalo.cse.cse486586.simpledht;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.StringTokenizer;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

	Context rcontext;
	ContentResolver conRes;

	static String emulNum;
	static String successor;
	static String predecessor;
	static Boolean largestFlag;
	static Boolean smallestFlag;
	static String queryValue;
	static int gcount = 0;
	static int dcount = 0;

	private static final String KEY_S = "key";
	private static final String VALUE_S = "value";
	static String[] cols = { KEY_S, VALUE_S };
	static MatrixCursor globalCursor = new MatrixCursor(cols);

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		rcontext = getContext();
		emulNum = portStr;
		conRes = rcontext.getContentResolver();
		// initiate the server thread
		Log.i("MSG","Oncreate of "+emulNum);
		Thread serv = new Server();
		serv.start();
		return false;
	}
	//delete function for deleting selection= * , @ and key
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		dcount=0;
		String key=selection;
		if(successor==null && predecessor==null){
			if (selection.equals("@")||selection.equals("*")) {
				String[] savFiles = rcontext.fileList();
				for (int i = 0; i < savFiles.length; i++)
					rcontext.deleteFile(savFiles[i]);
			}
			else
				rcontext.deleteFile(key);
		}
		else if (selection != null && selection.length() == 1) {
			if (selection.equals("@")||selection.equals("*")) {
				String[] savFiles = rcontext.fileList();
				for (int i = 0; i < savFiles.length; i++)
					rcontext.deleteFile(savFiles[i]);
			}
			if (selection.equals("*")) {
				new Thread(new Client(emulNum,Integer.parseInt(successor) * 2, 10)).start();
				while(dcount==1){}
			}
		}
		else if(selection != null && selection.length() >1){
			try {
				Boolean flag1=genHash(key).compareTo(genHash(emulNum)) <= 0;
				Boolean flag2=genHash(key).compareTo(genHash(predecessor)) > 0;
				Boolean flag3=genHash(key).compareTo(genHash(emulNum)) > 0;
				if (smallestFlag == true) {
					if (flag1||flag2)
						rcontext.deleteFile(key);
					else if (flag3)
						new Thread(new Client(key + ":" + emulNum,Integer.parseInt(successor) * 2, 11)).start();
				} else {
					if (flag1) {
						if (flag2)
							rcontext.deleteFile(key);
						else
							new Thread(new Client(key + ":" + emulNum,Integer.parseInt(predecessor) * 2, 11)).start();
					} else if (flag3)
						new Thread(new Client(key + ":" + emulNum,Integer.parseInt(successor) * 2, 11)).start();
				}
			}
			catch (NoSuchAlgorithmException e) {
				Log.i("MSG","delete:Error in delete");
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	public void actualInsert(String key, String value) {
		try {
			FileOutputStream fos = rcontext.openFileOutput(key,Context.MODE_PRIVATE);
			OutputStreamWriter osw = new OutputStreamWriter(fos);
			osw.write(value);
			osw.flush();
			osw.close();
		} catch (Exception e) {
			Log.i("MSG", "Exception e = " + e);
		}
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = (String) values.get(KEY_S);
		String value = (String) values.get(VALUE_S);
		try {

			if(successor==null && predecessor==null)
				actualInsert(key, value);
			else {
				Boolean flag1= genHash(key).compareTo(genHash(predecessor)) > 0;
				Boolean flag2=genHash(key).compareTo(genHash(emulNum))<= 0;
				Boolean flag3=genHash(key).compareTo(genHash(emulNum)) > 0;

				if (smallestFlag == true) {
					if (flag1||flag2)
						actualInsert(key, value);
					else if (flag3)
						new Thread(new Client(key + ":" + value,Integer.parseInt(successor) * 2, 6)).start();
				} else {
					if (flag2) {
						if (flag1)
							actualInsert(key, value);
						else
							new Thread(new Client(key + ":" + value,Integer.parseInt(predecessor) * 2, 6)).start();
					} else if (flag3)
						new Thread(new Client(key + ":" + value,Integer.parseInt(successor) * 2, 6)).start();
				}
			}
		}
		catch (NoSuchAlgorithmException e) {
			Log.i("Insert","Error in insert");
		}
		getContext().getContentResolver().notifyChange(uri, null);
		return uri;
	}

	String[] actualQuery(String key) {
		String fname = key;
		String value;
		try {
			FileInputStream fin = rcontext.openFileInput(fname);
			InputStreamReader inpReader = new InputStreamReader(fin);
			BufferedReader br = new BufferedReader(inpReader);
			value = br.readLine();
		} catch (Exception e) {
			Log.i("MSG", "Exception e = " + e);
			value = null;
		}
		String[] row = { key, value };
		return row;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,String sortOrder) {
		MatrixCursor localCursor = new MatrixCursor(cols);
		queryValue = null;
		gcount=0;
		String key=selection;
		if(successor==null && predecessor==null){
			if (selection.equals("@")||selection.equals("*")) {
				String[] savFiles = rcontext.fileList();
				for (int i = 0; i < savFiles.length; i++)
					localCursor.addRow(actualQuery(savFiles[i]));
			}
			else{
				String[] res=actualQuery(key);
				localCursor.addRow(res);
			}
		}
		else if (selection != null && selection.length() == 1) {
			if (selection.equals("@")) {
				String[] savFiles = rcontext.fileList();
				for (int i = 0; i < savFiles.length; i++) {
					localCursor.addRow(actualQuery(savFiles[i]));
				}
			}
			if (selection.equals("*")) {
				if(emulNum.equals(successor) && successor.equals(predecessor)){
					String[] savFiles = rcontext.fileList();
					for (int i = 0; i < savFiles.length; i++)
						localCursor.addRow(actualQuery(savFiles[i]));
				}
				else{
					new Thread(new Client(emulNum,Integer.parseInt(successor) * 2, 8)).start();
					while (gcount ==0) {}
					return globalCursor;
				}
			}
		}
		else {
			try {
				Boolean flag1=genHash(key).compareTo(genHash(emulNum)) <= 0;
				Boolean flag2=genHash(key).compareTo(genHash(predecessor)) > 0;
				Boolean flag3=genHash(key).compareTo(genHash(emulNum)) > 0;
				if (smallestFlag == true) {
					if (flag1||flag2) {
						String[] row = actualQuery(selection);
						localCursor.addRow(row);
					} else if (flag3) {
						new Thread(new Client(key + ":" + emulNum,Integer.parseInt(successor) * 2, 7)).start();
						while (queryValue == null) {}
						String[] row = { key, queryValue };
						localCursor.addRow(row);
					}
				} else {
					if (flag1) {
						if (flag2) {
							String[] row = actualQuery(key);
							localCursor.addRow(row);
						} else {
							new Thread(new Client(key + ":" + emulNum,Integer.parseInt(predecessor) * 2, 7)).start();
							while (queryValue == null) {}
							String[] row = { key, queryValue };
							localCursor.addRow(row);
						}
					} else if (flag3) {
						new Thread(new Client(key + ":" + emulNum,Integer.parseInt(successor) * 2, 7)).start();
						while (queryValue == null) {}
						String[] row = { key, queryValue };
						localCursor.addRow(row);
					}
				}
			}
			catch (NoSuchAlgorithmException e) {
				Log.i("MSG", "query: Error!\n");
				e.printStackTrace();
			}
		}
		return localCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		return 0;
	}

	public class Client implements Runnable {
		String msg, TAG = "MSG";
		int type, port;

		Client(String msg, int port, int msgType) {
			this.msg = msg;
			this.port = port;
			type = msgType;
		}
		@Override
		public void run() {
			Socket clSock;
			try {
				// connect to server
				clSock = new Socket("10.0.2.2", port);
				// send the message to server
				PrintWriter sendData = new PrintWriter(clSock.getOutputStream());
				if (type == 1) // join  node request
					sendData.println("J" + msg);
				else if (type == 2)// update node request
					sendData.println("U" + msg);
				else if (type == 3) // update only successor
					sendData.println("S" + msg);
				else if (type == 4) // update only predecessor
					sendData.println("P" + msg);
				else if (type == 5) // query response
					sendData.println("^" + msg);
				else if (type == 6) // insert
					sendData.println("I" + msg);
				else if (type == 7) // query
					sendData.println("Q" + msg);
				else if (type == 8)// gdump
					sendData.println("*" + msg+getldump2());
				else if (type == 9)// gdump reply
					sendData.println("(" + msg);
				else if (type == 10)// delete
					sendData.println("-" + msg);//msg=originport
				else if (type == 11)// delete key
					sendData.println("#" + msg);//msg=originport
				sendData.flush();
				sendData.close();
				clSock.close();
			} catch (NumberFormatException e) {
				Log.i("MSG", "Client: Number format Exception!\n");
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				Log.i("MSG", "Client: I/O error occured when creating the socket!\n");
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			}
		}
	}

	class Server extends Thread {

		public void run() {
			try {
				if (emulNum.equals("5554")) {
					successor = "5554";
					predecessor = "5554";
					largestFlag = true;
					smallestFlag = true;
				}
				else
					new Thread(new Client(emulNum, 11108, 1)).start();
				// open connection on port 10000
				ServerSocket serSock = new ServerSocket(10000);
				while (true) {
					Socket recvSock = serSock.accept();// listen for client
					InputStreamReader readStream = new InputStreamReader(recvSock.getInputStream());// get the message
					BufferedReader recvInp = new BufferedReader(readStream);
					String recvMsg = recvInp.readLine();
					char msgType=recvMsg.charAt(0);// recognise message type
					// join request
					if(msgType=='J'){
						joinNode(recvMsg.substring(1));//recvMsg contains emulNumbr who wants to join
					}// insert
					else if(msgType=='I'){
						String passKeyVal=recvMsg.substring(1);
						Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
						String[] strSplit = passKeyVal.split(":");
						String key = strSplit[0];
						String value = strSplit[1];
						ContentValues cv = new ContentValues();
						cv.put("key", key);
						cv.put("value", value);
						conRes.insert(mUri, cv);
					}// query
					else if(msgType=='Q'){
						Thread thread = new queryKey(recvMsg.substring(1));//send msg as key:originport
						thread.start();
					}// UPDATES predecessor and successor of the node
					else if(msgType=='U'){
						String updateMsg=recvMsg.substring(1);
						String[] strParts = updateMsg.split(":");
						predecessor = strParts[0];
						successor = strParts[1];
						largestFlag= strParts[2].equals("1");
						smallestFlag = strParts[3].equals("1");
					}// set successor
					else if(msgType=='S'){
						String successorMsg=recvMsg.substring(1);
						successor = successorMsg;
					}// set predecessor
					else if(msgType=='P'){
						String predecessorMsg=recvMsg.substring(1);
						predecessor = predecessorMsg;
					}//query response
					else if(msgType=='^')
						queryValue = recvMsg.substring(1);
					else if(msgType=='*')//gdump
						getldump(recvMsg.substring(1));
					else if(msgType=='-'){//delete all dht ENTRY
						String originPort=recvMsg.substring(1);
						String sel = "@";
						Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
						if(originPort==emulNum){
							dcount=1;
						}else{
							conRes.delete(mUri,sel,null);
							new Thread(new Client(originPort,Integer.parseInt(successor) * 2, 10)).start();
						}
					}//delete a particular key
					else if(msgType=='#'){
						Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
						String[] strSplit = recvMsg.substring(1).split(":");
						String key = strSplit[0];
						conRes.delete(mUri,key,null);//it queries the key on its on dump
					}
					recvSock.close();
				}
			} catch (IOException e) {
				Log.i("MSG", "Server: I/O error occured when creating the socket!\n");
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
	}

	//join node request when a new emulator wants to join the ring server is: 5554 and receives all the join request
	void joinNode(String newNode) throws NoSuchAlgorithmException {
		String currNode = emulNum;
		int tempSmall = 0, tempLarge = 0;
		Boolean flag1=genHash(newNode).compareTo(genHash(currNode)) > 0;
		Boolean flag2=genHash(newNode).compareTo(genHash(successor)) < 0;
		Boolean flag3=genHash(newNode).compareTo(genHash(predecessor)) < 0;
		Boolean flag4=genHash(newNode).compareTo(genHash(currNode)) < 0;
		Boolean flag5=genHash(newNode).compareTo(genHash(predecessor))> 0;
		if (flag1) {
			if (largestFlag == true||flag2) {
				if (largestFlag == true) {
					largestFlag = false;
					tempLarge = 1;
				}
				new Thread(new Client(currNode + ":" + successor + ":"+ tempLarge + ":" + tempSmall,
						Integer.parseInt(newNode) * 2, 2)).start();//type 2 is update node predecessor,successor
				new Thread(new Client(newNode,Integer.parseInt(successor) * 2, 4)).start();//type 4 will update only predecessor
				successor = newNode;
			}
			else if (flag3) {
				new Thread(new Client(newNode,Integer.parseInt(successor) * 2, 1)).start();
			}
			else
				new Thread(new Client(newNode,Integer.parseInt(successor) * 2, 1)).start();
		}
		else if (flag4) {
			if (smallestFlag == true||flag5) {
				if (smallestFlag == true) {
					smallestFlag = false;
					tempSmall = 1;
				}
				new Thread(new Client(predecessor + ":" + currNode + ":"+ tempLarge + ":" + tempSmall,
						Integer.parseInt(newNode) * 2, 2)).start();
				new Thread(new Client(newNode,Integer.parseInt(predecessor) * 2, 3)).start();//updates the successor
				predecessor = newNode;
			}
			else if (flag3) {
				new Thread(new Client(newNode,Integer.parseInt(predecessor) * 2, 1)).start();
			}
		}
	}

	// query key
	class queryKey extends Thread {
		String msg;
		queryKey(String msg) {
			this.msg = msg;
		}
		public void run() {
			Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
			String[] strSplit = msg.split(":");
			String key = strSplit[0];
			String orginPort = strSplit[1];
			Cursor resultCursor = conRes.query(mUri, null, key, null, null);//it queries the key on its on dump
			int valIndex = resultCursor.getColumnIndex("value");
			resultCursor.moveToFirst();
			String value = resultCursor.getString(valIndex);
			resultCursor.close();
			new Thread(new Client(value,Integer.parseInt(orginPort) * 2, 5)).start();
			//type 5 FOR query response ie @,stores the value in queryvalue
		}
	}



	// build Uri
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	//function for calculating genHash
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}


}*/
