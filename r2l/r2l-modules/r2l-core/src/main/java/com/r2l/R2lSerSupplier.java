package com.r2l;

import java.io.Serializable;
import java.util.function.Supplier;

public interface R2lSerSupplier<T> extends Serializable, Supplier<T> {

}
