package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;

import java.io.*;

import android.content.Context;

import java.util.ArrayList;


/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * <p/>
 * Please read:
 * <p/>
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * <p/>
 * before you start to get yourself familiarized with ContentProvider.
 * <p/>
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 *
 * @author stevko
 */
public class GroupMessengerProvider extends ContentProvider {

    static final String TAG = GroupMessengerProvider.class.getSimpleName();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String filename = values.getAsString("key");
        String stringvalue = values.getAsString("value");
        // Log.e(TAG, filename);
      //  File newFile = new File(filename);
        //Log.e("Sonty", values.toString());
        //Log.e("Code","Filename  : "+newFile.getName());
        //Log.e("Code","File path  : "+newFile.getAbsolutePath());
        FileOutputStream fos;

        try {


            fos = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
            //OutputStreamWriter sw = new OutputStreamWriter(fos);
                //tring message =  values.getAsString("value");

           // while(message != null && !message.isEmpty()) {


                fos.write(stringvalue.getBytes());



                fos.close();

        } catch (Exception e) {
            Log.e(TAG, "File write failed due to : " + e);
        }

        /*


         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */
        //Log.v("insert", values.toString());
        return uri;

    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        String[] columns = {"key", "value"};
        //selection = "key";
        String message;
        MatrixCursor cursor = new MatrixCursor(columns);
        //cursor.addRow(columns);

        ///*File file  = new File(selection);

/*
        Log.e("Query", "Filename  : " + selection);
        Log.e("Query", "Filename from file object : " + file.getName());

        Log.e("Query","File path  : "+file.getAbsolutePath());
*/


        //if(file != null) {
        //Log.e(TAG, "File Name is: "+selection);
        try {
            FileInputStream input = getContext().openFileInput(selection);
            BufferedReader br1 = new BufferedReader(new InputStreamReader(input));

            while ((message = br1.readLine()) != null) {
                //Log.e(TAG, "query: ", message);
                //message = br1.readLine();
                cursor.addRow(new String[]{selection, message});
                //Log.e("Query", "File rows  : " + message);

               // br1.close();

            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        //}
        //cursor.moveToFirst();

        /*

         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */
        //Log.v("query", selection);
        return cursor;
    }
}

