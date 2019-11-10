package com.r2l;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import com.r2l.spi.MasterCacheEventProcessorProvider;

public abstract class MasterCacheEventProcessor {
	@SuppressWarnings("unchecked")
	public static void main(String... args) {
		int argIndex = 0;
		int tableId = Integer.parseInt(args[argIndex++]);
		String consumeConnectionString = args[argIndex++];
		String consumeTopic = args[argIndex++];
		String consumerGroup = args[argIndex++];
		int produceTopics = Integer.parseInt(args[argIndex++]);
		int[] produceTopicNo = new int[produceTopics];
		String[] produceConnectionString = new String[produceTopics];
		String[] produceTopic = new String[produceTopics];
		Map<String, PulsarClient> clients = new HashMap<>();
		List<PulsarClientException> errors = new ArrayList<>();
		PulsarClient consumeClient = null;
		Consumer<byte[]> consumer;
		PulsarClient[] produceClients = new PulsarClient[produceTopics];
		Producer<byte[]>[] producers = new Producer[produceTopics];

		for (int i = 0; i < produceTopics; i++) {
			produceTopicNo[i] = Integer.parseInt(args[argIndex++]);
			produceConnectionString[i] = args[argIndex++];
			produceTopic[i] = args[argIndex++];
		}

		consumeClient = clients.computeIfAbsent(consumeConnectionString, _key -> create(_key, errors));
		if (errors.size() > 0) {
			errors.get(0).printStackTrace();
			System.exit(9);
			return;
		}
		try {
			consumer = consumeClient.newConsumer().topic(consumeTopic).subscriptionName(consumerGroup).subscribe();
		} catch (PulsarClientException e) {
			e.printStackTrace();
			System.exit(9);
			return;
		}
		for (int i = 0; i < produceTopics; i++) {
			produceClients[i] = clients.computeIfAbsent(produceConnectionString[i], _key -> create(_key, errors));
			if (errors.size() > 0) {
				errors.get(0).printStackTrace();
				System.exit(9);
				return;
			}
			try {
				producers[i] = produceClients[i].newProducer().topic(produceTopic[i]).create();
			} catch (PulsarClientException e) {
				e.printStackTrace();
				System.exit(9);
				return;
			}
		}
		MasterCacheEventProcessorProvider provider = null;
		for (MasterCacheEventProcessorProvider _provider : ServiceLoader.load(MasterCacheEventProcessorProvider.class)) {
			if (_provider.tableId() == tableId) {
				provider = _provider;
				break;
			}
		}
		if (provider == null) {
			System.exit(3);
			return;
		}
		MasterCacheEventProcessor processor = provider.provide();
		processor.consumeClient = consumeClient;
		processor.consumer = consumer;
		processor.produceTopicNo = produceTopicNo;
		processor.produceClients = produceClients;
		processor.producers = producers;
	}

	private static PulsarClient create(String connectionString, List<PulsarClientException> errors) {
		try {
			return PulsarClient.builder().serviceUrl(connectionString).build();
		} catch (PulsarClientException e) {
			errors.add(e);
			return null;
		}
	}

	private PulsarClient consumeClient;
	private Consumer<byte[]> consumer;
	private int[] produceTopicNo;
	private PulsarClient[] produceClients;
	private Producer<byte[]>[] producers;

}
