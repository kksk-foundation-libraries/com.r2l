package com.r2l.source.func;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.r2l.R2lSerFunc;
import com.r2l.R2lSerSupplier;
import com.r2l.model.common.colf.OutboxEventCollectResult;
import com.r2l.source.SourceProcessException;

public class OutboxEventRemover implements R2lSerFunc<OutboxEventCollectResult, Long> {
	private static final long serialVersionUID = -3369022246247789417L;

	private static final String SQL_REMOVE_COMMAND = String.join("\n" //
			, "DELETE" //
			, "FROM" //
			, "  OUTBOX_EVENT OBE" //
			, "WHERE" //
			, "    OBE.EVENT_ID = ?" //
	);

	private final R2lSerSupplier<Connection> connectionSupplier;
	private Connection connection;
	private PreparedStatement ps;

	public OutboxEventRemover(R2lSerSupplier<Connection> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	@Override
	public Long apply(OutboxEventCollectResult t) {
		try {
			if (connection == null) {
				connection = connectionSupplier.get();
				connection.setAutoCommit(false);
			}
			if (ps == null) {
				ps = connection.prepareStatement(SQL_REMOVE_COMMAND);
			} else {
				ps.clearParameters();
			}
			ps.setBigDecimal(1, new BigDecimal(t.getEventId()));
			long result = ps.executeUpdate();
			connection.commit();
			return result;
		} catch (SQLException e) {
			throw new SourceProcessException(e);
		}
	}
}
