package com.alibaba.dubbo.rpc.cluster.loadbalance;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.SPI;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;

import java.util.List;
import java.util.Random;


/**
 * @author taylor
 */
@SPI
public class AppointLoadBalance extends AbstractLoadBalance {
    public static final String NAME = "mybalance";
    private final Random random = new Random();

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        int length = invokers.size();
        int totalWeight = 0;
        boolean sameWeight = true;

        int offset;
        int i;
        for (offset = 0; offset < length; ++offset) {
            i = this.getWeight((Invoker) invokers.get(offset), invocation);
            totalWeight += i;
            if (sameWeight && offset > 0 && i != this.getWeight((Invoker) invokers.get(offset - 1), invocation)) {
                sameWeight = false;
            }
        }

        if (totalWeight > 0 && !sameWeight) {
            offset = this.random.nextInt(totalWeight);

            for (i = 0; i < length; ++i) {
                offset -= this.getWeight((Invoker) invokers.get(i), invocation);
                if (offset < 0) {
                    return invokers.get(i);
                }
            }
        }
        return invokers.get(this.random.nextInt(length));
    }
}