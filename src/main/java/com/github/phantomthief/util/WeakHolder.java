/**
 * 
 */
package com.github.phantomthief.util;

import java.lang.ref.WeakReference;
import java.util.function.Supplier;

/**
 * @author w.vela
 */
public final class WeakHolder<T> {

    private volatile WeakReference<T> ref;

    private final Supplier<T> supplier;

    /**
     * @param supplier
     */
    private WeakHolder(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public static final <T> WeakHolder<T> of(Supplier<T> supplier) {
        return new WeakHolder<>(supplier);
    }

    public T get() {
        T result;
        if (ref == null || (result = ref.get()) == null) {
            synchronized (this) {
                if (ref == null || (result = ref.get()) == null) {
                    result = supplier.get();
                    ref = new WeakReference<>(result);
                }
            }
        }
        return result;
    }
}
