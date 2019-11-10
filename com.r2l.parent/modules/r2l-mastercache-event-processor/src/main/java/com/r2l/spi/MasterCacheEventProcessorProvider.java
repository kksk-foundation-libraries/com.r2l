package com.r2l.spi;

import com.r2l.MasterCacheEventProcessor;

public interface MasterCacheEventProcessorProvider {
	int tableId();

	MasterCacheEventProcessor provide();
}
