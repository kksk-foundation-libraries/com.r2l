package com.r2l.source.func;

import java.util.Iterator;

import com.r2l.R2lSerFunc;
import com.r2l.model.common.colf.OutboxCommandCollectResult;
import com.r2l.model.common.colf.OutboxEventCollectResult;

public class OutboxCommandSpliter implements R2lSerFunc<OutboxCommandCollectResult, Iterable<OutboxEventCollectResult>> {
	private static final long serialVersionUID = 5147116448246320359L;

	@Override
	public Iterable<OutboxEventCollectResult> apply(OutboxCommandCollectResult t) {
		return new Iterable<OutboxEventCollectResult>() {

			@Override
			public Iterator<OutboxEventCollectResult> iterator() {
				return new Iterator<OutboxEventCollectResult>() {
					int pos = 0;

					@Override
					public boolean hasNext() {
						return pos < t.maxSerialNo;
					}

					@Override
					public OutboxEventCollectResult next() {
						if (!hasNext()) {
							return null;
						}
						return new OutboxEventCollectResult() //
								.withMarkPoint(t.getMarkPoint()) //
								.withTransactionId(t.getTransactionId()) //
								.withMaxEventId(t.getMaxEventId()) //
								.withMaxSerialNo(t.getMaxSerialNo()) //
								.withSerialNo(++pos) //
						;
					}
				};
			}
		};
	}

}
