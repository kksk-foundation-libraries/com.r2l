package com.r2l;

import java.util.Iterator;

import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import com.r2l.model.common.colf.OutboxCommand;
import com.r2l.model.common.colf.OutboxData;
import com.r2l.model.common.colf.OutboxEvent;

public class EventProcessor {
	public void initialize(PCollection<OutboxCommand> upstream) {
		upstream //
				.apply("split event from command", FlatMapElements.via(new Command2Events())) //
				.apply("fill event", MapElements.via(new FillEventInfo())) //
				.apply("publish and delete event", MapElements.via(new PublishAndDeleteEventInfo())) //
		;
	}

	private static class Command2Events extends SimpleFunction<OutboxCommand, Iterable<OutboxData>> {
		/** serialVersionUID */
		private static final long serialVersionUID = -7054260249262015178L;

		@Override
		public Iterable<OutboxData> apply(OutboxCommand input) {
			return new Iterable<OutboxData>() {
				@Override
				public Iterator<OutboxData> iterator() {
					return new Iterator<OutboxData>() {
						int pos = 0;

						@Override
						public OutboxData next() {
							if (pos < input.maxSerialNo) {
								pos++;
								return new OutboxData().withCommand(input).withEvent(new OutboxEvent().withTransactionId(input.getTransactionId()).withSerialNo(pos));
							}
							return null;
						}

						@Override
						public boolean hasNext() {
							return pos < input.maxSerialNo;
						}
					};
				}
			};
		}
	}

	private static class FillEventInfo extends SimpleFunction<OutboxData, OutboxData> {
		/** serialVersionUID */
		private static final long serialVersionUID = -4140467169864672548L;

		@Override
		public OutboxData apply(OutboxData input) {
			return super.apply(input);
		}
	}

	private static class PublishAndDeleteEventInfo extends SimpleFunction<OutboxData, OutboxData> {
		/** serialVersionUID */
		private static final long serialVersionUID = 6123249288010187385L;

		@Override
		public OutboxData apply(OutboxData input) {
			return super.apply(input);
		}
	}
}
