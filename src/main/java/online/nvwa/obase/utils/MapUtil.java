package online.nvwa.obase.utils;

import java.util.Map;

public class MapUtil {
    /**
     * 两个一层集合相加
     * @param a
     * @param b
     * @return
     */
    public static Map<String, Integer> addToMap1(
            Map<String, Integer> a,
            Map<String, Integer> b){
        b.entrySet().stream().forEach(e->{
            a.put(e.getKey(), a.getOrDefault(e.getKey(),0) + e.getValue());
        });

        return a;
    }

    /**
     * 两个两层集合相加
     * @param a
     * @param b
     * @return
     */
    public static Map<String, Map<String, Integer>> addToMap2(
            Map<String, Map<String, Integer>> a,
            Map<String, Map<String, Integer>> b){
        b.entrySet().stream().forEach(e->{
            String cateory = e.getKey();
            if(!a.containsKey(cateory)){
                a.put(cateory, e.getValue());
            }else{
                Map<String, Integer> target = a.get(cateory);
                addToMap1(target, e.getValue());
            }
        });

        return a;
    }
}
