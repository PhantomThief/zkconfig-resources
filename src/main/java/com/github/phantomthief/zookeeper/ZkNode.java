package com.github.phantomthief.zookeeper;

import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;
import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import java.util.function.Supplier;

/**
 * @author w.vela
 * Created on 2017-01-19.
 */
public class ZkNode<T> implements Supplier<T> {

    private final T obj;
    private final boolean exist;

    ZkNode(T obj, boolean exist) {
        this.obj = obj;
        this.exist = exist;
    }

    @Override
    public T get() {
        return obj;
    }

    public boolean nodeExists() {
        return exist;
    }

    @Override
    public String toString() {
        return reflectionToString(this, SHORT_PREFIX_STYLE);
    }
}
