package com.r2l;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import com.r2l.constants.RdbAction;
import com.r2l.model.RdbEvent;
import com.r2l.schema.RdbRecord;

public abstract class RdbEventProvider<Source> implements AutoCloseable {
	protected abstract RdbRecord convert(Source source);

	private final int tableId;
	private final String pulsarBrokerRootUrl;
	private final String topic;
	private final AtomicBoolean connected = new AtomicBoolean(false);
	private PulsarClient pulsarClient;
	private Producer<byte[]> pulsarProducer;
	private final AtomicLong sequencer = new AtomicLong();

	protected RdbEventProvider(short nodeId, int tableId, String pulsarBrokerRootUrl, String topic) {
		this.tableId = tableId;
		this.pulsarBrokerRootUrl = pulsarBrokerRootUrl;
		this.topic = topic;
		long ts = (Instant.now().getEpochSecond() - Instant.parse("2019-01-01T00:00:00.00Z").getEpochSecond()) & 0xffffffL;
		sequencer.set(nodeId << 52 | ts << 28);
	}

	private void open() throws Exception {
		if (connected.get())
			return;
		synchronized (connected) {
			if (connected.get())
				return;
			try {
				pulsarClient = PulsarClient.builder().serviceUrl(pulsarBrokerRootUrl).build();
				pulsarProducer = pulsarClient.newProducer().topic(topic).create();
			} catch (PulsarClientException e) {
				throw e;
			}
			connected.set(true);
		}
	}

	public void inserted(Source source) throws Exception {
		act(RdbAction.INSERT, source);
	}

	public void insertedOrUpdated(Source source) throws Exception {
		act(RdbAction.INSERT_OR_UPDATE, source);
	}

	public void updated(Source source) throws Exception {
		act(RdbAction.UPDATE, source);
	}

	public void deleted(Source source) throws Exception {
		act(RdbAction.DELETE, source);
	}

	@Override
	public void close() throws Exception {
		if (!connected.get())
			return;
		synchronized (connected) {
			if (!connected.get())
				return;
			try {
				pulsarProducer.close();
				pulsarClient.close();
			} catch (PulsarClientException e) {
				throw e;
			}
			connected.set(false);
			return;
		}
	}

	private void act(byte rdbAction, Source source) throws Exception {
		open();
		RdbEvent rdbEvent = new RdbEvent() //
				.withEventId(sequencer.incrementAndGet()) //
				.withAction(rdbAction) //
				.withRecord(convert(source).marshal()) //
				.withTableId(tableId) //
		;
		pulsarProducer.send(rdbEvent.marshal());
	}
}
