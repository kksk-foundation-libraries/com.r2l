package com.r2l;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.google.common.collect.ImmutableMap;
import com.r2l.model.common.colf.OutboxCommon;

public class EventProcessor {
	private static final KV<byte[], byte[]> NO_RECORDS = KV.of(new byte[] {}, new byte[] {});
	private final String uri;
	private final String user;
	private final String password;
	private final String commandKafkaServers;
	private final String commandTopic;
	private final String eventKafkaServers;
	private final String eventTopic;

	public EventProcessor(String uri, String user, String password, String commandKafkaServers, String commandTopic, String eventKafkaServers, String eventTopic) {
		this.uri = uri;
		this.user = user;
		this.password = password;
		this.commandKafkaServers = commandKafkaServers;
		this.commandTopic = commandTopic;
		this.eventKafkaServers = eventKafkaServers;
		this.eventTopic = eventTopic;
	}

	public Pipeline initialize() {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		pipeline //
				.apply("consume command", KafkaIO.readBytes() //
						.withBootstrapServers(commandKafkaServers) //
						.withTopic(commandTopic) //
						.withConsumerConfigUpdates(ImmutableMap.of("compression.type", "gzip")) //
				) //
				.apply("map to command", //
						FlatMapElements //
								.into(TypeDescriptors.kvs(TypeDescriptor.of(byte[].class), TypeDescriptor.of(byte[].class))) //
								.via(kafkaRecord -> {
									OutboxCommon outboxCommon = new OutboxCommon().unmarshal(kafkaRecord.getKV().getValue());
									
									return Arrays.asList(NO_RECORDS);
								}) //
				) //
		;
		return pipeline;
	}
}
