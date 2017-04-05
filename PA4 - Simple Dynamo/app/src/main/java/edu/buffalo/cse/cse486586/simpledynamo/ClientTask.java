package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.content.SharedPreferences;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;


// Created by tushar on 4/22/16.




public class ClientTask extends AsyncTask<Message, Void, Void> implements Constants {
    static final String TAG = ClientTask.class.getSimpleName();
    @Override
    protected Void doInBackground(Message... msg) {

        Message message = msg[0];

        switch (message.getMessageType()){
            case Insert:    //insert(message);
                break;
            case SingleQuery: //singleQuery(message);
                break;
            case GlobalDump: //queryGlobalDump(message);
                break;
            case SingleDelete: //singleDelete(message);
                break;
            case GlobalDelete: //executeGlobalDelete(message);
                break;
            case Recover: recover();
                break;
        }

        return null;
    }

    private String insertValue(Message message){
        Log.e(TAG, "***********************insertValue begins for Recovery************************");
        String key = message.getKey();
        String value = message.getValue();
        Context context = SimpleDynamoProvider.getProviderContext();
        SharedPreferences sharedPref = context.getSharedPreferences(PREFERENCE_NAME, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();
        editor.putString(key, value);
        editor.commit();
        Log.e(TAG, "====================================");
        Log.e(TAG, "Key - Value pair inserted -> key: " + key + " - value: " + value);
        Log.e(TAG, "====================================");
        Log.e(TAG, "***********************insertValue ends for Recovery************************");

        return "SUCCESS";
    }

    private List<NodeDetails> getReplicaNodeDetails(NodeDetails curentNode) {
        Log.e(TAG, "***********************getReplicaNodeDetails() begins ************************");
        List<NodeDetails> replicaNodeList = new ArrayList<NodeDetails>();
        NodeDetails prevNode1 = new NodeDetails();
        NodeDetails prevNode2 = new NodeDetails();

        for (NodeDetails node: SimpleDynamoProvider.chordNodeList) {
            if(node.getSuccessorPort().equals(curentNode.getPort())){
                prevNode1 = node;
            }
        }

        for (NodeDetails node: SimpleDynamoProvider.chordNodeList) {
            if(node.getSuccessorPort().equals(prevNode1.getPort())){
                prevNode2 = node;
            }
        }
        replicaNodeList.add(prevNode1);
        replicaNodeList.add(prevNode2);
        replicaNodeList.add(curentNode);
        Log.e(TAG, "====================================");
        Log.e(TAG, "Current node: " + curentNode.getNodeIdHash() + " should contain data within following ranges: ");
        Log.e(TAG, "Range 1 : " + prevNode2.getPredecessorNodeIdHash()+ " - " + prevNode2.getNodeIdHash());
        Log.e(TAG, "Range 2 : " + prevNode1.getPredecessorNodeIdHash()+ " - " + prevNode1.getNodeIdHash());
        Log.e(TAG, "Range 3 : " + curentNode.getPredecessorNodeIdHash()+ " - " + curentNode.getNodeIdHash());
        Log.e(TAG, "====================================");
        Log.e(TAG, "***********************getReplicaNodeDetails() ends ************************");
        return replicaNodeList;
    }


    private void recover(){
        Log.e(TAG, "***********************recover() begins in Client Task************************");

        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;
        Socket socket;
        List<NodeDetails> replicaNodeList = getReplicaNodeDetails(SimpleDynamoProvider.myNodeDetails);
        Message recoveryMessage = new Message();
        recoveryMessage.setMessageType(MessageType.Recover);
        List<Message> messageList = new ArrayList<Message>();
        List<Message> globalMessageList = new ArrayList<Message>();
        for (String port: REMOTE_PORTS) {
            Log.e(TAG,"====================================");
            try {
                if(port.equals(SimpleDynamoProvider.myNodeDetails.getPort())){
                    continue;
                }
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
                outputStream = new ObjectOutputStream(socket.getOutputStream());
                inputStream = new ObjectInputStream(socket.getInputStream());
                outputStream.writeObject(recoveryMessage);
                outputStream.flush();
                Log.e(TAG, "Waiting for Data from port: " + port + "...");
                messageList = (List<Message>)inputStream.readObject();
                Log.e(TAG,"Data arrived from port : "+port+": " +messageList.toString());
                globalMessageList.addAll(messageList);
            } catch (Exception e) {
                Log.e(TAG, "************************Exception in getGlobalDump()******************");
                e.printStackTrace();
                Log.e(TAG, "***********************Exception message ends************************");
            }
            Log.e(TAG,"====================================");
        }

        NodeDetails node1 = replicaNodeList.get(0);
        NodeDetails node2 = replicaNodeList.get(1);
        NodeDetails node3 = replicaNodeList.get(2);

        for (Message message : globalMessageList) {
            try {
                String key = message.getKey();
                String keyHash = Util.genHash(key);
                if (Util.lookup(keyHash, node1) || Util.lookup(keyHash, node2) || Util.lookup(keyHash, node3)){
                    insertValue(message);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        SimpleDynamoProvider.isInitialized = true;
        Log.e(TAG, "***********************recover() ends in Client Task************************");
    }

}

