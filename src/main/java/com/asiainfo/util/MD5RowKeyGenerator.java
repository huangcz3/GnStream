package com.asiainfo.util;

import java.security.MessageDigest;

/**
 * Created by migle on 2016/9/27.
 */
public class MD5RowKeyGenerator {
    private MessageDigest md = null;

    public Object generate(String oriRowKey)
    {
        return generatePrefix(oriRowKey) + oriRowKey;
    }

    /**
     * 生成完整的md5串
     * @param oriRowKey
     * @return
     */
    public synchronized String getMD5(String oriRowKey){
        char[] hexDigits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'b', 'd', 'e', 'f' };
        try {
            byte[] btInput = oriRowKey.getBytes();
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            mdInst.update(btInput);
            byte[] md = mdInst.digest();
            int j = md.length;
            char[] str = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++){
                byte byte0 = md[i];
                str[(k++)] = hexDigits[(byte0 >>> 4 & 0xF)];
                str[(k++)] = hexDigits[(byte0 & 0xF)];
            }
            return new String(str);
        } catch (Exception e) {}
        return "";
    }

    /**
     * 生成md5的2、4、6位
     * @param oriRowKey
     * @return
     */
    public String generatePrefix(String oriRowKey) {
        String result = getMD5(oriRowKey);
        return result.substring(1, 2) + result.substring(3, 4) + result.substring(5, 6);
    }
}
