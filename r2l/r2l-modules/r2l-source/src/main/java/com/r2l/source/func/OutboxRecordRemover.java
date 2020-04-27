package com.r2l.source.func;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.r2l.R2lSerFunc;
import com.r2l.R2lSerSupplier;
import com.r2l.model.common.colf.OutboxRecordCollectResult;
import com.r2l.source.SourceProcessException;
import com.r2l.source.spi.OutboxRecordConverterProvider;

public class OutboxRecordRemover implements R2lSerFunc<OutboxRecordCollectResult, Long> {
	private static final long serialVersionUID = -3369022246247789417L;

	private final R2lSerSupplier<Connection> connectionSupplier;
	private final ConcurrentMap<String, OutboxRecordConverter> converters = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, PreparedStatement> statements = new ConcurrentHashMap<>();
	private Connection connection;
	private PreparedStatement ps;

	public OutboxRecordRemover(R2lSerSupplier<Connection> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	@Override
	public Long apply(OutboxRecordCollectResult t) {
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
				connection.setAutoCommit(false);
			}
			ps = statements.computeIfAbsent(t.getEventTarget(), _key -> {
				try {
					return connection.prepareStatement(converter.deleteStatement());
				} catch (SQLException e) {
					throw new SourceProcessException(e);
				}
			});
			ps.clearParameters();
			ps.setBigDecimal(1, new BigDecimal(t.getEventId()));
			long result = ps.executeUpdate();
			connection.commit();
			return result;
		} catch (SQLException e) {
			throw new SourceProcessException(e);
		}
	}
}
