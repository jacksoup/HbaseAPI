package org.caesar.hbase.util;

/**
 * Created by caesar.zhu on 2016/11/25.
 */
public class CommonUtil {
    /**
     * 过滤特殊用于分隔作用的特殊字符
     * @param value
     * @return
     */
    public static String filterSpecChar(String value){
        return value.replaceAll(Constant.COLLECTION_SPLIT_CH,"").replaceAll(Constant.MAP_KV_CH,"");
    }
}
