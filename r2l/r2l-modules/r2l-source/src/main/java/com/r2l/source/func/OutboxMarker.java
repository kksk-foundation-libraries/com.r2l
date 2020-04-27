package com.r2l.source.func;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.r2l.R2lSerFunc;
import com.r2l.R2lSerSupplier;
import com.r2l.source.SourceProcessException;

public class OutboxMarker implements R2lSerFunc<Long, Long> {
	private static final long serialVersionUID = 7958947395716491781L;

	private static final String SQL_MARK_OUTBOX = String.join("\n" //
			, "INSERT INTO" //
			, "  OUTBOX_MARK" //
			, "(" //
			, "  OUTBOX_MARK_POINT" //
			, ", TRANSACTION_ID" //
			, ")" //
			, "SELECT" //
			, "  SEQ_OUTBOX_MARK_POINT.NEXTVAL" //
			, ", OBC.TRANSACTION_ID" //
			, "FROM" //
			, "  OUTBOX_COMMAND OBC" //
			, ", OUTBOX_MARK OBM" //
			, "WHERE" //
			, "    OBM.TRANSACTION_ID(+) = OBC.TRANSACTION_ID" //
			, "AND OBM.TRANSACTION_ID IS NULL" //
			, "AND ROWNUM <= ?" //
			, "ORDER BY" //
			, "  OBC.MAX_EVENT_ID" //
	);

	private final R2lSerSupplier<Connection> connectionSupplier;
	private final int batchSize;
	private Connection connection;
	private PreparedStatement ps;

	public OutboxMarker(R2lSerSupplier<Connection> connectionSupplier, int batchSize) {
		this.connectionSupplier = connectionSupplier;
		this.batchSize = batchSize;
	}

	@Override
	public Long apply(Long t) {
		try {
			if (connection == null) {
				connection = connectionSupplier.get();
				connection.setAutoCommit(false);
			}
			if (ps == null) {
				ps = connection.prepareStatement(SQL_MARK_OUTBOX);
			} else {
				ps.clearParameters();
			}
			ps.setInt(1, batchSize);
			long results = ps.executeUpdate();
			connection.commit();
			return results;
		} catch (SQLException e) {
			throw new SourceProcessException(e);
		}
	}
}
