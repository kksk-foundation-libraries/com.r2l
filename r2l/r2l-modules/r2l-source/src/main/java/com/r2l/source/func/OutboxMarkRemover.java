package com.r2l.source.func;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.r2l.R2lSerFunc;
import com.r2l.R2lSerSupplier;
import com.r2l.model.common.colf.OutboxCommandCollectResult;
import com.r2l.source.SourceProcessException;

public class OutboxMarkRemover implements R2lSerFunc<OutboxCommandCollectResult, Long> {
	private static final long serialVersionUID = 7958947395716491781L;

	private static final String SQL_REMOVE_MARK = String.join("\n" //
			, "DELETE" //
			, "FROM" //
			, "  OUTBOX_MARK OBM" //
			, "WHERE" //
			, "    OBM.OUTBOX_MARK_POINT = ?" //
	);

	private final R2lSerSupplier<Connection> connectionSupplier;
	private Connection connection;
	private PreparedStatement ps;

	public OutboxMarkRemover(R2lSerSupplier<Connection> connectionSupplier) {
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
				ps = connection.prepareStatement(SQL_REMOVE_MARK);
			} else {
				ps.clearParameters();
			}
			ps.setBigDecimal(1, new BigDecimal(t.getMarkPoint()));
			long result = ps.executeUpdate();
			connection.commit();
			return result;
		} catch (SQLException e) {
			throw new SourceProcessException(e);
		}
	}
}
