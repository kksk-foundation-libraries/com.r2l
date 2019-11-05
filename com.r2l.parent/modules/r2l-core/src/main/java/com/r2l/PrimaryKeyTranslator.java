package com.r2l;

import java.util.function.Function;

import com.r2l.schema.PrimaryKey;
import com.r2l.schema.RdbRecord;

public interface PrimaryKeyTranslator<RdbRecordType extends RdbRecord> extends Function<RdbRecordType, PrimaryKey> {

}
