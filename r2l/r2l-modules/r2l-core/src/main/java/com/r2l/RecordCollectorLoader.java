package com.r2l;

import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RecordCollectorLoader {
	private final ConcurrentMap<String, RecordCollector> repositry = new ConcurrentHashMap<>();

	public RecordCollector get(String eventTarget) {
		return repositry.computeIfAbsent(eventTarget, _eventTarget -> {
			for (RecordCollector recordCollector : ServiceLoader.load(RecordCollector.class)) {
				if (_eventTarget.equals(recordCollector.eventTarget())) {
					return recordCollector;
				}
			}
			return null;
		});
	}
}
