package com.r2l;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.ImmutableMap;
import com.r2l.model.common.colf.OutboxCommand;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public class CommandCollector {
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

	private static final OutboxCommand BLANK_VALUE = new OutboxCommand();
	private static final int params = 100;
	private static final String SELECT_QUERY = String.join(" ", //
			"SELECT", //
			"    OBX.TRANSACTION_ID AS TRANSACTION_ID,", //
			"    OBX.MAX_EVENT_ID   AS MAX_EVENT_ID,", //
			"    OBX.MAX_SERIAL_NO  AS MAX_SERIAL_NO", //
			"FROM", //
			"    OUTBOX_COMMAND OBX", //
			"WHERE", //
			"    OBX.TRANSACTION_ID NOT IN (" + String.join(", ", Arrays.asList(new String[params]).stream().map(s -> "?").collect(Collectors.toList()).toArray(new String[params])) + ")", //
			"ORDER BY", //
			"    MAX_EVENT_ID", //
			"" //
	);
	private static final String DELETE_QUERY = String.join(" ", //
			"DELETE", //
			"FROM", //
			"    OUTBOX_COMMAND OBX", //
			"WHERE", //
			"    OBX.TRANSACTION_ID = ?", //
			"" //
	);

	public void initialize(Pipeline pipeline, String zkServers, String zkPath, long interval) {
		initialize(pipeline //
				.apply("cock", PipelineCock.open(zkServers, zkPath, interval)) //
		);
	}

	public void initialize(PCollection<? extends Object> upstream) {
		upstream //
				.apply("collect outbox command", MapElements.via(new Collect(uri, user, password))) //
				.apply("skip by no record.", Filter.by(command -> !BLANK_VALUE.equals(command))) //
				.apply("publish and delete command", MapElements.via(new PublishAndDelete(uri, user, password, commandKafkaServers, commandTopic))) //
		;
	}

	private static class Collect extends SimpleFunction<Object, OutboxCommand> {
		/** serialVersionUID */
		private static final long serialVersionUID = 1353996667266729508L;
		private final String uri;
		private final String user;
		private final String password;

		private final Queue<String> lastTransactionId = new LinkedList<>();

		public Collect(String uri, String user, String password) {
			this.uri = uri;
			this.user = user;
			this.password = password;
		}

		@Override
		public OutboxCommand apply(Object input) {
			try (Connection connection = DataSourceFactory.get(uri, user, password).getConnection()) {
				PreparedStatement ps = connection.prepareStatement(SELECT_QUERY);
				String[] arr = new ArrayList<>(lastTransactionId).toArray(new String[1]);
				for (int i = 0; i < params; i++) {
					if (i < arr.length) {
						ps.setString(i + 1, arr[i]);
					} else {
						ps.setString(i + 1, " ");
					}
				}
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					String transactionId = rs.getString("TRANSACTION_ID");
					OutboxCommand command = new OutboxCommand() //
							.withTransactionId(transactionId) //
							.withMaxEventId(rs.getString("MAX_EVENT_ID")) //
							.withMaxSerialNo(rs.getInt("MAX_SERIAL_NO")) //
					;
					lastTransactionId.offer(transactionId);
					while (lastTransactionId.size() > params) {
						lastTransactionId.poll();
					}
					return command;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return BLANK_VALUE;
		}
	}

	private static class PublishAndDelete extends SimpleFunction<OutboxCommand, Object> {
		/** serialVersionUID */
		private static final long serialVersionUID = -8448424208163215041L;

		private final String uri;
		private final String user;
		private final String password;
		private final String commandKafkaServers;
		private final String commandTopic;
		private boolean initialized = false;
		private final AtomicReference<FluxSink<OutboxCommand>> sinkRef = new AtomicReference<>();
		private final Lock lock = new ReentrantLock();

		public PublishAndDelete(String uri, String user, String password, String commandKafkaServers, String commandTopic) {
			this.uri = uri;
			this.user = user;
			this.password = password;
			this.commandKafkaServers = commandKafkaServers;
			this.commandTopic = commandTopic;
		}

		@Override
		public Object apply(OutboxCommand input) {
			if (!initialized) {
				final KafkaSender<byte[], byte[]> sender = KafkaSender.create(SenderOptions.create(ImmutableMap.of( //
						ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, commandKafkaServers, //
						ProducerConfig.ACKS_CONFIG, "all" //
				)));
				sender //
						.send( //
								Flux //
										.<OutboxCommand> create(sink -> {
											sinkRef.set(sink);
											sink.onCancel(() -> {
												sinkRef.set(null);
											});
										}) //
										.map(command -> {
											return SenderRecord.create(new ProducerRecord<byte[], byte[]>(commandTopic, command.getTransactionId().getBytes(), command.marshal()), command);
										}) //
						) //
						.subscribe(result -> {
							try (Connection connection = DataSourceFactory.get(uri, user, password).getConnection()) {
								PreparedStatement ps = connection.prepareStatement(DELETE_QUERY);
								ps.setString(1, result.correlationMetadata().getTransactionId());
								ps.execute();
								connection.commit();
							} catch (SQLException e) {
								e.printStackTrace();
							} finally {
								lock.unlock();
							}

						}, t -> {
							lock.unlock();
						}) //
				;
				initialized = true;
			}
			lock.lock();
			sinkRef.get().next(input);
			lock.lock();
			lock.unlock();

			return BLANK_VALUE;
		}
	}
}
