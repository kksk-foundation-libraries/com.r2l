package com.r2l;

import java.io.Serializable;
import java.util.function.Function;

public interface R2lSerFunc<T, R> extends Serializable, Function<T, R> {

}
