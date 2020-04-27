package com.r2l;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class SourceConnector {
	private static String SQL_MARKING = String.join("\n" //
			, "INSERT INTO" //
			, "    OBX_MARK" //
			, "(" //
			, "    MARK_POINT" //
			, ",   TRANSACTION_ID" //
			, ")" //
			, "SELECT" //
			, "    SEQ_MARK_POINT.NEXTVAL" //
			, ",   OBC.TRANSACTION_ID" //
			, "FROM" //
			, "    OUTBOX_COMMAND OBC" //
			, "LEFT OUTER JOIN" //
			, "    OBX_MARK OBM" //
			, "ON" //
			, "    OBC.TRANSACTION_ID = OBM.TRANSACTION_ID" //
			, "WHERE" //
			, "    OBM.TRANSACTION_ID IS NULL" //
			, "AND ROWNUM <= ?" //
			, "OREDER BY" //
			, "    OBC.COMMAND_NO" //
	);

	private static String SQL_FIRST_MARK_POINT = String.join("\n" //
			, "SELECT" //
			, "    MIN(OBM.MARK_POINT)" //
			, "FROM" //
			, "    OBX_MARK" //
	);

	public static class Marker implements AutoCloseable {
		private final PreparedStatement ps;
		private final int batchSize;

		public Marker(Connection cn, int batchSize) throws SQLException {
			ps = cn.prepareStatement(SQL_MARKING);
			this.batchSize = batchSize;
		}

		public int mark() throws SQLException {
			ps.setInt(1, batchSize);
			int res = ps.executeUpdate();
			ps.clearParameters();
			return res;
		}

		@Override
		public void close() throws Exception {
			ps.close();
		}
	}

	public static class FirstMarkPoint implements AutoCloseable {
		private final PreparedStatement ps;

		public FirstMarkPoint(Connection cn) throws SQLException {
			ps = cn.prepareStatement(SQL_FIRST_MARK_POINT);
		}

		public BigDecimal get() throws SQLException {
			try (ResultSet rs = ps.executeQuery();) {
				if (!rs.next()) {
					return BigDecimal.ZERO;
				}
				return rs.getBigDecimal(1).subtract(BigDecimal.ONE);
			}
		}

		@Override
		public void close() throws Exception {
			ps.close();
		}
	}

	private String url, user, pass;
	private int batchSize;
	private Pipeline pipeline;
	private long interval = Long.MAX_VALUE;

	public void initialize() {
		PCollection<Long> stream = pipeline //
				.apply("generate sequence, interval:" + interval, GenerateSequence.from(0).withRate(1L, Duration.millis(interval))) //
		;
		stream //
				.apply("mark", MapElements.via(new SimpleFunction<Long, Integer>() {
					/** serialVersionUID */
					private static final long serialVersionUID = 5079507232700205936L;

					private Marker marker;

					@Override
					public Integer apply(Long input) {
						if (marker == null) {
							try {
								marker = new Marker(DataSourceFactory.get(url, user, pass).getConnection(), batchSize);
							} catch (SQLException e) {
							}
						}
						try {
							return marker.mark();
						} catch (SQLException e) {
							e.printStackTrace();
							return -1;
						}
					}
				})) //
		;
		stream //
				.apply("add first mark point", MapElements.via(new SimpleFunction<Long, BigDecimal>() {
					/** serialVersionUID */
					private static final long serialVersionUID = 7379246061278455020L;
					private BigDecimal firstMarkPoint = null;

					@Override
					public BigDecimal apply(Long input) {
						if (firstMarkPoint == null) {
							try (FirstMarkPoint fmp = new FirstMarkPoint(DataSourceFactory.get(url, user, pass).getConnection())) {
								firstMarkPoint = fmp.get();
							} catch (Exception e) {
								e.printStackTrace();
							} finally {
							}
						}
						return firstMarkPoint.add(BigDecimal.valueOf(input.longValue()));
					}
				})) //
		;
	}
}
