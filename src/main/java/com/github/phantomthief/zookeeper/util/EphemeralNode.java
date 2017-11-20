package com.github.phantomthief.zookeeper.util;

import javax.annotation.Nonnull;

/**
 * @author w.vela
 * Created on 2017-11-04.
 */
public interface EphemeralNode extends AutoCloseable { // just for alias

    void updateValue(@Nonnull byte[] value);
}
