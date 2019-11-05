package com.r2l;

import java.util.function.Function;

import com.r2l.schema.IndexKey;
import com.r2l.schema.RdbRecord;

public interface IndexKeyTranslator<RdbRecordType extends RdbRecord, IndexKeyType extends IndexKey> extends Function<RdbRecordType, IndexKeyType> {
}
