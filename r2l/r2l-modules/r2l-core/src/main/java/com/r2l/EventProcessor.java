package com.r2l;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.ImmutableMap;
import com.r2l.model.common.colf.OutboxCommand;
import com.r2l.model.common.colf.OutboxData;
import com.r2l.model.common.colf.OutboxEvent;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public class EventProcessor {
	private static final String SELECT_EVENT_QUERY = String.join(" ", //
			"SELECT", //
			"    OBX.EVENT_ID       AS EVENT_ID,", //
			"    OBX.TRANSACTION_ID AS TRANSACTION_ID,", //
			"    OBX.SERIAL_NO      AS SERIAL_NO,", //
			"    OBX.EVENT_TARGET   AS EVENT_TARGET,", //
			"    OBX.ACTION         AS ACTION", //
			"FROM", //
			"    OUTBOX_EVENT OBX", //
			"WHERE", //
			"    OBX.TRANSACTION_ID = ?", //
			"ORDER BY", //
			"    EVENT_ID", //
			"" //
	);
	private static final String DELETE_EVENT_QUERY = String.join(" ", //
			"DELETE", //
			"FROM", //
			"    OUTBOX_EVENT OBX", //
			"WHERE", //
			"    OBX.EVENT_ID = ?", //
			"" //
	);

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

	public void initialize(Pipeline pipeline) {
		initialize( //
				pipeline //
						.apply( //
								"consume kafka", //
								KafkaIO.readBytes() //
										.withBootstrapServers(commandKafkaServers) //
										.withTopics(Arrays.asList(commandTopic)) //
						) //
						.apply( //
								"consume kafka", //
								MapElements.via(new SimpleFunction<KafkaRecord<byte[], byte[]>, OutboxCommand>() {
									/** serialVersionUID */
									private static final long serialVersionUID = 6365470009853262145L;

									@Override
									public OutboxCommand apply(KafkaRecord<byte[], byte[]> input) {
										return new OutboxCommand().unmarshal(input.getKV().getValue());
									}
								}) //
						) //
		);
	}

	public void initialize(PCollection<OutboxCommand> upstream) {
		upstream //
				.apply("split event from command", FlatMapElements.via(new Command2Events(uri, user, password))) //
				.apply("append record info", MapElements.via(new AppendRecordInfo(uri, user, password))) //
				.apply("publish outbox data", MapElements.via(new PublishEventInfo(eventKafkaServers, eventTopic))) //
				.apply("delete record", MapElements.via(new DeletedRecordInfo(uri, user, password))) //
				.apply("delete event", MapElements.via(new DeleteEventInfo(uri, user, password))) //
		;
	}

	private static class Command2Events extends SimpleFunction<OutboxCommand, Iterable<OutboxData>> {
		/** serialVersionUID */
		private static final long serialVersionUID = -7054260249262015178L;
		private final String uri;
		private final String user;
		private final String password;

		public Command2Events(String uri, String user, String password) {
			this.uri = uri;
			this.user = user;
			this.password = password;
		}

		@Override
		public Iterable<OutboxData> apply(OutboxCommand input) {
			return new Iterable<OutboxData>() {
				@Override
				public Iterator<OutboxData> iterator() {
					final Closer closer = new Closer();
					try {
						Connection connection = DataSourceFactory.get(uri, user, password).getConnection();
						closer.setConnection(connection);
						PreparedStatement ps = connection.prepareStatement(SELECT_EVENT_QUERY);
						closer.setPs(ps);
						ps.setString(1, input.getTransactionId());
						ResultSet rs = ps.executeQuery();
						closer.setRs(rs);
						return new Iterator<OutboxData>() {
							boolean fetched = true;
							boolean hasNext = rs.next();

							@Override
							public OutboxData next() {
								if (hasNext()) {
									fetched = false;
									try {
										return new OutboxData() //
												.withCommand(input) //
												.withEvent( //
														new OutboxEvent() //
																.withEventId(rs.getBigDecimal("EVENT_ID").toString()) //
																.withTransactionId(input.getTransactionId()) //
																.withSerialNo(rs.getInt("SERIAL_NO")) //
																.withEventTarget(rs.getString("EVENT_TARGET")) //
																.withAction(rs.getByte("ACTION")) //
										) //
										;
									} catch (SQLException e) {
										return null;
									}
								} else {
									return null;
								}
							}

							@Override
							public boolean hasNext() {
								fetch();
								return hasNext;
							}

							private void fetch() {
								if (!fetched) {
									try {
										hasNext = rs.next();
									} catch (SQLException e) {
										hasNext = false;
									}
									fetched = true;
									if (!hasNext) {
										closer.close();
									}
								}
							}
						};
					} catch (SQLException e) {
						closer.close();
						return null;
					}
				}
			};
		}
	}

	private static class Closer {
		private Connection connection;
		private PreparedStatement ps;
		private ResultSet rs;

		public Closer() {
		}

		public void setConnection(Connection connection) {
			this.connection = connection;
		}

		public void setPs(PreparedStatement ps) {
			this.ps = ps;
		}

		public void setRs(ResultSet rs) {
			this.rs = rs;
		}

		public void close() {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	private static class AppendRecordInfo extends SimpleFunction<OutboxData, OutboxData> {
		/** serialVersionUID */
		private static final long serialVersionUID = -8271654303570838529L;
		private final String uri;
		private final String user;
		private final String password;
		private final RecordCollectorLoader loader = new RecordCollectorLoader();

		public AppendRecordInfo(String uri, String user, String password) {
			this.uri = uri;
			this.user = user;
			this.password = password;
		}

		@Override
		public OutboxData apply(OutboxData input) {
			final Closer closer = new Closer();
			try {
				RecordCollector collector = loader.get(input.getEvent().getEventTarget());
				Connection connection = DataSourceFactory.get(uri, user, password).getConnection();
				closer.setConnection(connection);
				PreparedStatement ps = collector.createQuery(connection);
				closer.setPs(ps);
				ps.setBigDecimal(1, new BigDecimal(input.getEvent().getEventId()));
				ResultSet rs = ps.executeQuery();
				input.withRecord(collector.map(rs));
			} catch (SQLException e) {
			} finally {
				closer.close();
			}

			return input;
		}
	}

	private static class PublishEventInfo extends SimpleFunction<OutboxData, OutboxData> {
		/** serialVersionUID */
		private static final long serialVersionUID = 1548671851747157160L;
		private final String eventKafkaServers;
		private final String eventTopic;
		private boolean initialized = false;
		private final AtomicReference<FluxSink<OutboxData>> sinkRef = new AtomicReference<>();
		private final Lock lock = new ReentrantLock();

		public PublishEventInfo(String eventKafkaServers, String eventTopic) {
			this.eventKafkaServers = eventKafkaServers;
			this.eventTopic = eventTopic;
		}

		@Override
		public OutboxData apply(OutboxData input) {
			if (!initialized) {
				Flux.<OutboxData> create(sink -> {
					sinkRef.set(sink);
					sink.onCancel(() -> {
						sinkRef.set(null);
					});
				});
				final KafkaSender<byte[], byte[]> sender = KafkaSender.create(SenderOptions.create(ImmutableMap.of( //
						ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, eventKafkaServers, //
						ProducerConfig.ACKS_CONFIG, "all" //
				)));
				sender //
						.send( //
								Flux //
										.<OutboxData> create( //
												sink -> {
													sinkRef.set(sink);
													sink.onCancel(() -> {
														sinkRef.set(null);
													});
												}) //
										.map(data -> SenderRecord.create(new ProducerRecord<>(eventTopic, data.getCommand().getTransactionId().getBytes(), data.marshal()), data)) //
						) //
						.subscribe(result -> {
							lock.unlock();
						}, t -> {
							lock.unlock();
						});
				;
				initialized = true;
			}
			lock.lock();
			sinkRef.get().next(input);
			lock.lock();
			lock.unlock();
			return input;
		}
	}

	private static class DeletedRecordInfo extends SimpleFunction<OutboxData, OutboxData> {
		/** serialVersionUID */
		private static final long serialVersionUID = -5559059665752331951L;
		private final String uri;
		private final String user;
		private final String password;
		private final RecordCollectorLoader loader = new RecordCollectorLoader();

		public DeletedRecordInfo(String uri, String user, String password) {
			this.uri = uri;
			this.user = user;
			this.password = password;
		}

		@Override
		public OutboxData apply(OutboxData input) {
			final Closer closer = new Closer();
			try {
				RecordCollector collector = loader.get(input.getEvent().getEventTarget());
				Connection connection = DataSourceFactory.get(uri, user, password).getConnection();
				closer.setConnection(connection);
				PreparedStatement ps = collector.createDeleteStatement(connection);
				closer.setPs(ps);
				ps.setBigDecimal(1, new BigDecimal(input.getEvent().getEventId()));
				ps.execute();
			} catch (SQLException e) {
			} finally {
				closer.close();
			}

			return input;
		}
	}

	private static class DeleteEventInfo extends SimpleFunction<OutboxData, OutboxData> {
		/** serialVersionUID */
		private static final long serialVersionUID = 6123249288010187385L;
		private final String uri;
		private final String user;
		private final String password;

		public DeleteEventInfo(String uri, String user, String password) {
			this.uri = uri;
			this.user = user;
			this.password = password;
		}

		@Override
		public OutboxData apply(OutboxData input) {
			final Closer closer = new Closer();
			try {
				Connection connection = DataSourceFactory.get(uri, user, password).getConnection();
				closer.setConnection(connection);
				PreparedStatement ps = connection.prepareStatement(DELETE_EVENT_QUERY);
				closer.setPs(ps);
				ps.setBigDecimal(1, new BigDecimal(input.getEvent().getEventId()));
				ps.execute();
			} catch (SQLException e) {
			} finally {
				closer.close();
			}

			return input;
		}
	}
}
