/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package javaapplication1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author hello
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        HashMap<String,List<String>> lstOut=new HashMap<String,List<String>>();
        HashMap<String,List<String>> lst=new HashMap<String,List<String>>();
        List<String> test=new ArrayList<String>();
        test.add("mon 1");
        test.add("mon 2");
        test.add("mon 3");
        test.add("mon 4");
        lst.put("mon",test);

        List<String> test2=new ArrayList<String>();
        test2.add("tues 1");
        test2.add("tues 2");
        test2.add("@mon");
        test2.add("@wed");
        test2.add("tues 3");
        test2.add("tues 4");
        lst.put("tues",test2);

        List<String> test3=new ArrayList<String>();
        test3.add("wed 1");
        test3.add("wed 2");
        test3.add("@mon");
        test3.add("wed 3");
        test3.add("wed 4");
        lst.put("wed",test3);
        
   Iterator it = lst.entrySet().iterator();
    while (it.hasNext()) {
        Map.Entry pair = (Map.Entry)it.next();
        System.out.println(pair.getKey() + " = " + pair.getValue());
        List<String> data=new ArrayList<String>();
        data=returnElements(lst, (String) pair.getKey());
        lstOut.put((String) pair.getKey(),data);
//        it.remove();
    }
   System.out.println("#########"+lstOut);
    }
    public static List<String> returnElements(HashMap<String,List<String>> lst,String name){
        List<String> test=lst.get(name);
        List<String> output=new ArrayList<String>();
            for(int i=0;i<test.size();i++){
                if(test.get(i).indexOf("@")==0){
                    String name1=test.get(i).substring(1,test.get(i).length() );
                    System.out.println("name1  "+name1);
                    List<String> temp=returnElements(lst,name1);
                    for(int j=0;j<temp.size();j++){
                         output.add(temp.get(j));
                   }
                }else{
                    output.add(test.get(i));
                }
            }
        return output;
    }
}
