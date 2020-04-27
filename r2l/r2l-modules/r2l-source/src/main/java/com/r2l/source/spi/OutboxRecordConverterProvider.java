package com.r2l.source.spi;

import com.r2l.source.func.OutboxRecordConverter;

public interface OutboxRecordConverterProvider {
	String tableName();

	OutboxRecordConverter provide();
}
