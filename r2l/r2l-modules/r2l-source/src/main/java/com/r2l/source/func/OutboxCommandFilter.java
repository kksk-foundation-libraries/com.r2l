package com.r2l.source.func;

import com.r2l.R2lSerPredicate;
import com.r2l.model.common.colf.OutboxCommandCollectResult;

public class OutboxCommandFilter implements R2lSerPredicate<OutboxCommandCollectResult> {

	private static final long serialVersionUID = 1491012244114949530L;

	@Override
	public boolean test(OutboxCommandCollectResult t) {
		return t != null && !OutboxCommandCollector.BLANK_VALUE.equals(t);
	}

}
