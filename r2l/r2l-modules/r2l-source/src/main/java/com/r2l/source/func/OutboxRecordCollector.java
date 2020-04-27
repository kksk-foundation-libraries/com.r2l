package com.r2l.source.func;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.r2l.R2lSerFunc;
import com.r2l.R2lSerSupplier;
import com.r2l.model.common.colf.OutboxEventCollectResult;
import com.r2l.model.common.colf.OutboxRecordCollectResult;
import com.r2l.source.SourceProcessException;
import com.r2l.source.spi.OutboxRecordConverterProvider;

public class OutboxRecordCollector implements R2lSerFunc<OutboxEventCollectResult, OutboxRecordCollectResult> {
	private static final long serialVersionUID = -6385801067013943511L;

	private final R2lSerSupplier<Connection> connectionSupplier;
	private final ConcurrentMap<String, OutboxRecordConverter> converters = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, PreparedStatement> statements = new ConcurrentHashMap<>();
	private Connection connection;
	private PreparedStatement ps;
	private ResultSet rs;

	public OutboxRecordCollector(R2lSerSupplier<Connection> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	@Override
	public OutboxRecordCollectResult apply(OutboxEventCollectResult t) {
		try {
			final OutboxRecordConverter converter = converters.computeIfAbsent(t.getEventTarget(), _key -> {
				for (OutboxRecordConverterProvider provider : ServiceLoader.load(OutboxRecordConverterProvider.class)) {
					if (t.getEventTarget().equalsIgnoreCase(provider.tableName())) {
						return provider.provide();
					}
				}
				return null;
			});
			if (connection == null) {
				connection = connectionSupplier.get();
				connection.setAutoCommit(true);
			}
			ps = statements.computeIfAbsent(t.getEventTarget(), _key -> {
				try {
					return connection.prepareStatement(converter.query(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				} catch (SQLException e) {
					throw new SourceProcessException(e);
				}
			});
			ps.clearParameters();
			ps.setBigDecimal(1, new BigDecimal(t.getEventId()));
			rs = ps.executeQuery();
			byte[] record = converter.toBytes(rs);
			rs.close();
			rs = null;
			return new OutboxRecordCollectResult() //
					.withMarkPoint(t.getMarkPoint()) //
					.withTransactionId(t.getTransactionId()) //
					.withMaxEventId(t.getMaxEventId()) //
					.withMaxSerialNo(t.getMaxSerialNo()) //
					.withEventId(t.getEventId()) //
					.withSerialNo(t.getSerialNo()) //
					.withEventTarget(t.getEventTarget()) //
					.withAction(t.getAction()) //
					.withRecord(record) //
			;
		} catch (SQLException e) {
			throw new SourceProcessException(e);
		}
	}

}
