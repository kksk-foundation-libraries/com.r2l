package com.r2l;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.common.collect.ImmutableMap;
import com.r2l.model.common.colf.OutboxCommon;

public class CommandCollector {
	private static final KV<byte[], byte[]> NO_RECORDS = KV.of(new byte[] {}, new byte[] {});
	private final String uri;
	private final String user;
	private final String password;
	private final String commandKafkaServers;
	private final String commandTopic;

	public CommandCollector(String uri, String user, String password, String commandKafkaServers, String commandTopic) {
		this.uri = uri;
		this.user = user;
		this.password = password;
		this.commandKafkaServers = commandKafkaServers;
		this.commandTopic = commandTopic;
	}

	public Pipeline initialize() {
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		pipeline //
				.apply("unbound sequencer", GenerateSequence.from(0)) //
				.apply("collect command", //
						MapElements //
								.into(TypeDescriptors.kvs(TypeDescriptor.of(byte[].class), TypeDescriptor.of(byte[].class))) //
								.via(l -> {
									DataSource dataSource = DataSourceFactory.get(uri, user, password);
									try (Connection connection = dataSource.getConnection()) {
										PreparedStatement ps1 = connection.prepareStatement("SELECT TXN_ID, OBX_ID, OBX_SEQ FROM OUTBOX_COMMON OBX INNER JOIN (SELECT MIN(OBX_ID) AS MIN_ID FROM OUTBOX_COMMON) M_OBX ON OBX.OBX_ID = M_OBX.MIN_ID FOR UPDATE NOWAIT");
										ResultSet rs = ps1.executeQuery();
										if (rs.next()) {
											String txnId = rs.getString("TXN_ID");
											String id = rs.getString("OBX_ID");
											int seq = rs.getInt("OBX_SEQ");
											OutboxCommon outboxCommon = new OutboxCommon().withTxnId(txnId).withId(id).withSeq(seq);
											PreparedStatement ps2 = connection.prepareStatement("DELETE FROM OUTBOX_COMMON WHERE TXN_ID = ?");
											ps2.setString(1, txnId);
											ps2.execute();
											return KV.of(txnId.getBytes(), outboxCommon.marshal());
										}
										connection.commit();
									} catch (SQLException e) {
										e.printStackTrace();
									}
									return NO_RECORDS;
								}) //
				) //
				.apply("skip by no record.", Filter.by(kv -> !NO_RECORDS.equals(kv))) //
				.apply("write to kafka", KafkaIO.<byte[], byte[]>write() //
						.withBootstrapServers(commandKafkaServers) //
						.withTopic(commandTopic) //
						.withKeySerializer(ByteArraySerializer.class) //
						.withValueSerializer(ByteArraySerializer.class) //
						.withProducerConfigUpdates(ImmutableMap.of("compression.type", "gzip")) //
				) //
		;
		return pipeline;
	}
}
