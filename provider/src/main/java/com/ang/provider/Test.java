package com.ang.provider;

import java.io.IOException;
import java.util.Enumeration;

/**
 * @author xiaolu.zhang
 * @desc:
 * @date: 2018/3/1 14:59
 */
public class Test {
    public static void main(String[] args) throws IOException {
        Enumeration urls = ClassLoader.getSystemResources("C:\\DRIVERS");
        if (urls.hasMoreElements()) {
            System.out.println("yes");
        }
        System.out.println(urls);
    }
}
