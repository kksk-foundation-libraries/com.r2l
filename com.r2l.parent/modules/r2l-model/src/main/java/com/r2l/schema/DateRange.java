package com.r2l.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@AllArgsConstructor
@EqualsAndHashCode(doNotUseGetters = true)
public class DateRange {
	private final long begin;
	private final long end;

	public boolean contains(long date) {
		return begin <= date && date < end;
	}
}
