package com.r2l;

import java.util.function.Function;

import com.r2l.schema.DateRange;
import com.r2l.schema.RdbRecord;

public interface DateRangeTranslator<RdbRecordType extends RdbRecord> extends Function<RdbRecordType, DateRange> {
}
