package edu.buffalo.cse.cse486586.groupmessenger2;

import android.util.Log;

import java.util.Comparator;

/**
 * Created by sahil on 2/28/16.
 */
public class customcomparator implements Comparator<message_obj>
{

    public int compare(message_obj x, message_obj y)
    {
//        Log.d("comp1",Integer.parseInt(x.proposed_id)+"");
//        Log.d("comp2",Integer.parseInt(y.proposed_id)+"");
//        Log.d("comp1",Integer.parseInt(x.proposed_from_port)+"");
//        Log.d("comp2",Integer.parseInt(y.proposed_from_port)+"");

        if(Integer.parseInt(x.proposed_id)>Integer.parseInt(y.proposed_id))
        {
//            Log.d("comparatorcatch2","here-2-2-2");
            return 1;
        }
        else if(Integer.parseInt(x.proposed_id)<Integer.parseInt(y.proposed_id))
        {
//            Log.d("comparatorcatch2","here-1-1-1-1");
            return -1;
        }else{

        if(Integer.parseInt(x.proposed_from_port)>Integer.parseInt(y.proposed_from_port))
        {
//            Log.d("comparatorcatch2","here1111");
            return 1;
        }
        else if(Integer.parseInt(x.proposed_from_port)<Integer.parseInt(y.proposed_from_port)) {
//            Log.d("comparatorcatch2", "here22222");
            return -1;
        }
//            Log.d("comparatorcatch2","readched");
        return 0;
        }

    }


}