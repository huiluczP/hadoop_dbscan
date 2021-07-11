import com.huiluczP.corecluster.CoreClusterMergeAdaptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;

public class SimpleTest {
    public static void main(String[] args){
        String r = "0       1 2 12";
        StringTokenizer s = new StringTokenizer(r);
        System.out.println(s.countTokens());
        while(s.hasMoreTokens()){
            System.out.println(s.nextToken());
        }

        ArrayList<Integer> intList = new ArrayList<Integer>();
        intList.add(2);
        intList.add(8);
        intList.add(1);
        intList.add(0);
        intList.add(5);
        intList.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if(o1 < o2)
                    return -1;
                else if(o1 > o2)
                    return 1;
                else
                    return 0;
            }
        });
        for(int i=0;i<intList.size();i++){
            System.out.println(intList.get(i));
        }

        int[] isUsed = new int[3];
        for(int i=0;i<3;i++){
            System.out.println(isUsed[i]);
        }

        ArrayList<String> l1 = new ArrayList<>();
        ArrayList<String> l2 = new ArrayList<>();
        ArrayList<String> l3 = new ArrayList<>();
        ArrayList<String> l4 = new ArrayList<>();
        l1.add("0");
        l1.add("2");
        l1.add("3");
        l1.add("12");

        l2.add("1");
        l2.add("3");
        l2.add("12");

        l3.add("10");

        l4.add("4");
        l4.add("6");
        l4.add("7");

        ArrayList<ArrayList<String>> listList = new ArrayList<ArrayList<String>>();
        listList.add(l1);
        listList.add(l2);
        listList.add(l3);
        listList.add(l4);
        ArrayList<String> mergeResult = CoreClusterMergeAdaptor.onlyCoreMerge(listList);
        for(String mr:mergeResult){
            System.out.println(mr);
        }
    }
}
