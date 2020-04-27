package com.r2l.source.func;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.r2l.R2lSerFunc;
import com.r2l.R2lSerSupplier;
import com.r2l.model.common.colf.OutboxCommandCollectResult;
import com.r2l.source.SourceProcessException;

public class OutboxCommandRemover implements R2lSerFunc<OutboxCommandCollectResult, Long> {
	private static final long serialVersionUID = -3369022246247789417L;

	private static final String SQL_REMOVE_COMMAND = String.join("\n" //
			, "DELETE" //
			, "FROM" //
			, "  OUTBOX_COMMAND OBC" //
			, "WHERE" //
			, "    OBC.TRANSACTION_ID = ?" //
	);

	private final R2lSerSupplier<Connection> connectionSupplier;
	private Connection connection;
	private PreparedStatement ps;

	public OutboxCommandRemover(R2lSerSupplier<Connection> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	@Override
	public Long apply(OutboxCommandCollectResult t) {
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
			ps.setString(1, t.getTransactionId());
			long result = ps.executeUpdate();
			connection.commit();
			return result;
		} catch (SQLException e) {
			throw new SourceProcessException(e);
		}
	}
}
