package org.apache.kyuubi.web.utils;

import java.util.HashMap;
import java.util.Map;

public class CollectionUtils {

    public static <T, K> Map<T, K> mapOf(T k1, K v1, T k2, K v2) {
        HashMap<T, K> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }
}
