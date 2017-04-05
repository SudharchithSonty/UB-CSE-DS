

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
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

/*public class SimpleDhtProvider extends ContentProvider {

    Context context;
    ContentResolver conRes;
    static String myemulatornumber;
    static String successornode;
    static String predecessornode;
    static Boolean largestFlag;
    static Boolean smallestFlag;
    static String querytype;
    static int counter1 = 0; //gcount//
    static int counter2 = 0; //dcount//
    private static final String KEY = "key";
    private static final String VALUE = "value";
    static String[] ColumnNames = { KEY, VALUE };
    static MatrixCursor globalCursor = new MatrixCursor(ColumnNames);
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    int counter = 0;
    Boolean isorigin;
    String[] ports = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        context = getContext();
        myemulatornumber = portStr;
        conRes = context.getContentResolver();
        // initiate the server thread
        Log.i("MSG", "Oncreate of " + myemulatornumber);
        try {

            ServerSocket socket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    //delete function for deleting selection= * , @ and key
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        counter2=0;
        String key=selection;
        String[] savFiles = context.fileList();
        if(successornode==null && predecessornode==null){
            if (selection.equals("@")||selection.equals("*")) {

                int i=0;
                while(i<savFiles.length)
                {
                    context.deleteFile(savFiles[i]);
                    i++;
                }

            }
            else
                context.deleteFile(key);
        }
        else if (selection != null && selection.length() == 1) {
            if (selection.equals("@")||selection.equals("*")) {
                int i=0;
                while(i<savFiles.length)
                {
                    context.deleteFile(savFiles[i]);
                    i++;
                }

            }
            if (selection.equals("*")) {
                new Thread(new Client(myemulatornumber,Integer.parseInt(successornode) * 2, 10)).start();
                while(counter2==1){}
            }
        }
        else if(selection != null && selection.length() >1){
            try {
                Boolean flag1,flag2,flag3;
                if(genHash(key).compareTo(genHash(myemulatornumber)) <= 0)
                {
                    flag1 = true;
                }
                else
                    flag1 =false;
                if(genHash(key).compareTo(genHash(predecessornode)) > 0)
                {
                    flag2 = true;
                }
                else
                    flag2 = false;
                if(genHash(key).compareTo(genHash(myemulatornumber)) > 0)
                {
                    flag3 =true;
                }
                else
                    flag3 =false;
                if (smallestFlag == true) {
                    if (flag1||flag2)
                        context.deleteFile(key);
                    else if (flag3)
                        new Thread(new Client(key + ":" + myemulatornumber,Integer.parseInt(successornode) * 2, 11)).start();
                } else {
                    if (flag1) {
                        if (flag2)
                            context.deleteFile(key);
                        else
                            new Thread(new Client(key + ":" + myemulatornumber,Integer.parseInt(predecessornode) * 2, 11)).start();
                    } else if (flag3)
                        new Thread(new Client(key + ":" + myemulatornumber,Integer.parseInt(successornode) * 2, 11)).start();
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


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = (String) values.get(KEY);
        String value = (String) values.get(VALUE);
        try {

            if(successornode==null && predecessornode==null)
                Insertfunction(key, value);
            else {
                Boolean flag1= genHash(key).compareTo(genHash(predecessornode)) > 0;
                Boolean flag2=genHash(key).compareTo(genHash(myemulatornumber))<= 0;
                Boolean flag3=genHash(key).compareTo(genHash(myemulatornumber)) > 0;

                if (smallestFlag == true) {
                    if (flag1||flag2)
                        Insertfunction(key, value);
                    else if (flag3)
                        new Thread(new Client(key + ":" + value,Integer.parseInt(successornode) * 2, 6)).start();
                } else {
                    if (flag2) {
                        if (flag1)
                            Insertfunction(key, value);
                        else
                            new Thread(new Client(key + ":" + value,Integer.parseInt(predecessornode) * 2, 6)).start();
                    } else if (flag3)
                        new Thread(new Client(key + ":" + value,Integer.parseInt(successornode) * 2, 6)).start();
                }
            }
        }
        catch (NoSuchAlgorithmException e) {
            Log.i("Insert","Error in insert");
        }
        getContext().getContentResolver().notifyChange(uri, null);
        return uri;
    }

    String[] QueryFunction(String key) {
        String filename = key;
        String value;
        try {
            FileInputStream fin = context.openFileInput(filename);
            InputStreamReader input = new InputStreamReader(fin);
            BufferedReader br = new BufferedReader(input);
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
        MatrixCursor localCursor = new MatrixCursor(ColumnNames);
        querytype = null;
        counter1=0;
        String key=selection;

        if(successornode==null && predecessornode==null){
            if (selection.equals("@")||selection.equals("*")) {
                String[] savFiles = context.fileList();
                for (int i = 0; i < savFiles.length; i++)
                    localCursor.addRow(QueryFunction(savFiles[i]));
            }
            else{
                String[] res=QueryFunction(key);
                localCursor.addRow(res);
            }
        }
        else if (selection != null && selection.length() == 1) {
            if (selection.equals("@")) {
                String[] savFiles = context.fileList();
                for (int i = 0; i < savFiles.length; i++) {
                    localCursor.addRow(QueryFunction(savFiles[i]));
                }
            }
            if (selection.equals("*")) {
                if(myemulatornumber.equals(successornode) && successornode.equals(predecessornode)){
                    String[] savFiles = context.fileList();
                    for (int i = 0; i < savFiles.length; i++)
                        localCursor.addRow(QueryFunction(savFiles[i]));
                }
                else{
                    Log.e("QUERY", "Error");
                    new Thread(new Client(myemulatornumber,Integer.parseInt(successornode) * 2, 8)).start();
                    while (counter1 ==0) {}
                    return globalCursor;
                }
            }
        }
        else {
            try {
                Boolean flag1=genHash(key).compareTo(genHash(myemulatornumber)) <= 0;
                Boolean flag2=genHash(key).compareTo(genHash(predecessornode)) > 0;
                Boolean flag3=genHash(key).compareTo(genHash(myemulatornumber)) > 0;
                if (smallestFlag == true) {
                    if (flag1||flag2) {
                        String[] row = QueryFunction(selection);
                        localCursor.addRow(row);
                    } else if (flag3) {
                        new Thread(new Client(key + ":" + myemulatornumber,Integer.parseInt(successornode) * 2, 7)).start();
                        while (querytype == null) {}
                        String[] row = { key, querytype };
                        localCursor.addRow(row);
                    }
                } else {
                    if (flag1) {
                        if (flag2) {
                            String[] row = QueryFunction(key);
                            localCursor.addRow(row);
                        } else {
                            new Thread(new Client(key + ":" + myemulatornumber,Integer.parseInt(predecessornode) * 2, 7)).start();
                            while (querytype == null) {}
                            String[] row = { key, querytype };
                            localCursor.addRow(row);
                        }
                    } else if (flag3) {
                        new Thread(new Client(key + ":" + myemulatornumber,Integer.parseInt(successornode) * 2, 7)).start();
                        while (querytype == null) {}
                        String[] row = { key, querytype };
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
            Socket socket;
            try {

                socket = new Socket("10.0.2.2", port);
                PrintWriter pw = new PrintWriter(socket.getOutputStream());
                if (type == 1) // join  node request
                    pw.println("join" + msg);
                else if (type == 2)// update node request
                    pw.println("U" + msg);
                else if (type == 3) // update only successor
                    pw.println("S" + msg);
                else if (type == 4) // update only predecessor
                    pw.println("P" + msg);
                else if (type == 5) // query response
                    pw.println("^" + msg);
                else if (type == 6) // insert
                    pw.println("I" + msg);
                else if (type == 7) // query
                    pw.println("Q" + msg);
                else if (type == 8)// gdump
                    pw.println("*" + msg+getldump2());
                else if (type == 9)// gdump reply
                    pw.println("(" + msg);
                else if (type == 10)// delete
                    pw.println("-" + msg);//msg=originport
                else if (type == 11)// delete key
                    pw.println("#" + msg);//msg=originport*//*

                pw.flush();
                pw.close();
                socket.close();
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        protected Void doInBackground(ServerSocket... sockets) {
            try {
                if (myemulatornumber.equals("5554")) {
                    successornode = "5554";
                    predecessornode = "5554";
                    largestFlag = true;
                    smallestFlag = true;
                }
                else
                    new Thread(new Client(myemulatornumber, 11108, 1)).start();
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
                        NodeJoin(recvMsg.substring(1));//recvMsg contains emulNumbr who wants to join
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
                        predecessornode = strParts[0];
                        successornode = strParts[1];
                        largestFlag= strParts[2].equals("1");
                        smallestFlag = strParts[3].equals("1");
                    }// set successor
                    else if(msgType=='S'){
                        String successorMsg=recvMsg.substring(1);
                        successornode = successorMsg;
                    }// set predecessor
                    else if(msgType=='P'){
                        String predecessorMsg=recvMsg.substring(1);
                        predecessornode = predecessorMsg;
                    }//query response
                    else if(msgType=='^')
                        querytype = recvMsg.substring(1);
                    else if(msgType=='*')//gdump
                        getldump(recvMsg.substring(1));
                    else if(msgType=='-'){//delete all dht ENTRY
                        String originPort=recvMsg.substring(1);
                        String sel = "@";
                        Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
                        if(originPort==myemulatornumber){
                            counter2=1;
                        }else{
                            conRes.delete(mUri,sel,null);
                            new Thread(new Client(originPort,Integer.parseInt(successornode) * 2, 10)).start();
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
            return null;
        }


        }


    //join node request when a new emulator wants to join the ring server is: 5554 and receives all the join request
    void NodeJoin(String newNode) throws NoSuchAlgorithmException {
        String currNode = myemulatornumber;
        int tempSmall = 0, tempLarge = 0;
        if (genHash(newNode).compareTo(genHash(currNode)) > 0) { //1
            if (largestFlag == true||genHash(newNode).compareTo(genHash(successornode)) < 0)  { //2
                if (largestFlag == true) {
                    largestFlag = false;
                    tempLarge = 1;
                }
                new Thread(new Client(currNode + ":" + successornode + ":"+ tempLarge + ":" + tempSmall,
                        Integer.parseInt(newNode) * 2, 2)).start();//type 2 is update node predecessornode,successornode
                new Thread(new Client(newNode,Integer.parseInt(successornode) * 2, 4)).start();//type 4 will update only predecessornode
                successornode = newNode;
            }
            else if (genHash(newNode).compareTo(genHash(predecessornode)) < 0) { //3
                new Thread(new Client(newNode,Integer.parseInt(successornode) * 2, 1)).start();
            }
            else
                new Thread(new Client(newNode,Integer.parseInt(successornode) * 2, 1)).start();
        }
        else if (genHash(newNode).compareTo(genHash(currNode)) < 0) { //4
            if (smallestFlag == true|| genHash(newNode).compareTo(genHash(predecessornode))> 0) { //5
                if (smallestFlag == true) {
                    smallestFlag = false;
                    tempSmall = 1;
                }
                new Thread(new Client(predecessornode + ":" + currNode + ":"+ tempLarge + ":" + tempSmall,
                        Integer.parseInt(newNode) * 2, 2)).start();
                new Thread(new Client(newNode,Integer.parseInt(predecessornode) * 2, 3)).start();//updates the successornode
                predecessornode = newNode;
            }
            else if (genHash(newNode).compareTo(genHash(predecessornode)) < 0)  { //3
                new Thread(new Client(newNode,Integer.parseInt(predecessornode) * 2, 1)).start();
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
            String[] strSplit = msg.split(":");
            String key = strSplit[0];
            String orginPort = strSplit[1];
            Cursor resultCursor = conRes.query(uri, null, key, null, null);//it queries the key on its on dump
            int valIndex = resultCursor.getColumnIndex("value");
            resultCursor.moveToFirst();
            String value = resultCursor.getString(valIndex);
            resultCursor.close();
            new Thread(new Client(value,Integer.parseInt(orginPort) * 2, 5)).start();
            //type 5 FOR query response ie @,stores the value in querytype
        }
    }

    public void Insertfunction(String key, String value) {
        try {
            FileOutputStream fos = context.openFileOutput(key,Context.MODE_PRIVATE);
            OutputStreamWriter writer = new OutputStreamWriter(fos);
            writer.write(value);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            Log.i("MSG", "Exception e = " + e);
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
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
    String getldump2() {
        String sel = "@";
        String ldump = "";
        Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
        Cursor resultCursor = conRes.query(mUri, null, sel, null, null);
        int keyIndex = resultCursor.getColumnIndex("key");
        int valueIndex = resultCursor.getColumnIndex("value");
        resultCursor.moveToFirst();
        while (!resultCursor.isAfterLast()) {
            String key = resultCursor.getString(keyIndex);
            String val = resultCursor.getString(valueIndex);
            String kvPair = ":"+key + "," + val;
            ldump = ldump + kvPair;
            resultCursor.moveToNext();
        }
        resultCursor.close();
        return ldump;
    }

    //used to get all the key,value pairs from whole dht and put in gcursor
    void getldump(String msg) {//msg is of form originport:ldump from originport
        String[] emptyCur={"",""};
        StringTokenizer sTok = new StringTokenizer(msg, ":");
        String originPort = sTok.nextToken();
        if(originPort.equals(myemulatornumber)){
            //termination condition for query as ldump contains port numbr only when all files are deleted
            if(msg.length()>4){
                String gDump=msg.substring(5);
                String[] strSplit = gDump.split(":");
                for(int i =0; i < strSplit.length ; i++) {
                    String temp = strSplit[i];
                    String[] strSplit2 = temp.split(",");
                    String key = strSplit2[0];
                    String value = strSplit2[1];
                    String[] row = { key, value };
                    globalCursor.addRow(row);
                }
            }
            else
                globalCursor.addRow(emptyCur);
            counter1=1;
        }
        else
            new Thread(new Client(msg, Integer.parseInt(successornode) * 2, 8)).start();
    }


}
*/



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
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;

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
        try {

            ServerSocket socket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    //delete function for deleting selection= * , @ and key
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        dcount=0;
        String key=selection;

        if(successor==null && predecessor==null){
            if (selection.equals("@")||selection.equals("*")) {
                String[] fileList = rcontext.fileList();
                for (int i = 0; i < fileList.length; i++)
                    rcontext.deleteFile(fileList[i]);
            }
            else
                rcontext.deleteFile(key);
        }
        else if (selection != null && selection.length() == 1) {
            if (selection.equals("@")||selection.equals("*")) {
                String[] fileList = rcontext.fileList();
                for (int i = 0; i < fileList.length; i++)
                    rcontext.deleteFile(fileList[i]);
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



    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = (String) values.get(KEY_S);
        String value = (String) values.get(VALUE_S);
        Boolean flag1, flag2,flag3;
        try {

            if(successor==null && predecessor==null)
            {
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
            else {

               flag1=genHash(key).compareTo(genHash(predecessor)) > 0;
                flag2=genHash(key).compareTo(genHash(emulNum))<= 0;
                flag3=genHash(key).compareTo(genHash(emulNum)) > 0;

                if (smallestFlag == true) {
                    if (flag1||flag2)
                    {
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

                    else if (flag3)
                        new Thread(new Client(key + ":" + value,Integer.parseInt(successor) * 2, 6)).start();
                } else {
                    if (flag2) {
                        if (flag1)
                        {
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
        String[] savFiles = rcontext.fileList();
        if(successor==null && predecessor==null){
            if (selection.equals("@")||selection.equals("*")) {

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

                for (int i = 0; i < savFiles.length; i++) {
                    localCursor.addRow(actualQuery(savFiles[i]));
                }
            }
            if (selection.equals("*")) {
                if(emulNum.equals(successor) && successor.equals(predecessor)){

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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        protected Void doInBackground(ServerSocket... sockets) {

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
                ServerSocket serSock = sockets[0];
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
            } return  null;
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

    //used to get all the key,value pairs from whole dht and put in gcursor
    void getldump(String msg) {//msg is of form originport:ldump from originport
        String[] emptyCur={"",""};
        StringTokenizer sTok = new StringTokenizer(msg, ":");
        String originPort = sTok.nextToken();
        if(originPort.equals(emulNum)){
            //termination condition for query as ldump contains port numbr only when all files are deleted
            if(msg.length()>4){
                String gDump=msg.substring(5);
                String[] strSplit = gDump.split(":");
                for(int i =0; i < strSplit.length ; i++) {
                    String temp = strSplit[i];
                    String[] strSplit2 = temp.split(",");
                    String key = strSplit2[0];
                    String value = strSplit2[1];
                    String[] row = { key, value };
                    globalCursor.addRow(row);
                }
            }
            else
                globalCursor.addRow(emptyCur);
            gcount=1;
        }
        else
            new Thread(new Client(msg, Integer.parseInt(successor) * 2, 8)).start();
    }

    //used to get local dump from an emulator and return the whole list as a string of key1,val1:key2,val2...
    String getldump2() {
        String sel = "@";
        String ldump = "";
        Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
        Cursor resultCursor = conRes.query(mUri, null, sel, null, null);
        int keyIndex = resultCursor.getColumnIndex("key");
        int valueIndex = resultCursor.getColumnIndex("value");
        resultCursor.moveToFirst();
        while (!resultCursor.isAfterLast()) {
            String key = resultCursor.getString(keyIndex);
            String val = resultCursor.getString(valueIndex);
            String kvPair = ":"+key + "," + val;
            ldump = ldump + kvPair;
            resultCursor.moveToNext();
        }
        resultCursor.close();
        return ldump;
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

