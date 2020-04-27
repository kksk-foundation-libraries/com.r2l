package com.r2l.source.func;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.r2l.R2lSerFunc;
import com.r2l.R2lSerSupplier;
import com.r2l.model.common.colf.OutboxCommandCollectResult;
import com.r2l.source.SourceProcessException;

public class OutboxCommandCollector implements R2lSerFunc<Long, OutboxCommandCollectResult> {
	private static final long serialVersionUID = -3369022246247789417L;

	private static final String SQL_MARK_OUTBOX = String.join("\n" //
			, "SELECT" //
			, "  OBM.OUTBOX_MARK_POINT" //
			, "FROM" //
			, "  OUTBOX_MARK OBM" //
			, "WHERE" //
			, "    ROWNUM <= ?" //
			, "ORDER BY" //
			, "  OBM.OUTBOX_MARK_POINT" //
	);

	public static final OutboxCommandCollectResult BLANK_VALUE = new OutboxCommandCollectResult();

	private final R2lSerSupplier<Connection> connectionSupplier;
	private final int fetchSize;
	private Connection connection;
	private PreparedStatement ps;
	private ResultSet rs;

	public OutboxCommandCollector(R2lSerSupplier<Connection> connectionSupplier, int fetchSize) {
		this.connectionSupplier = connectionSupplier;
		this.fetchSize = fetchSize;
	}

	@Override
	public OutboxCommandCollectResult apply(Long t) {
		try {
			if (rs == null) {
				if (connection == null) {
					connection = connectionSupplier.get();
					connection.setAutoCommit(false);
				}
				if (ps == null) {
					ps = connection.prepareStatement(SQL_MARK_OUTBOX, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				} else {
					ps.clearParameters();
				}
				ps.setInt(1, fetchSize);
				rs = ps.executeQuery();
			}
			OutboxCommandCollectResult result = BLANK_VALUE;
			if (rs.next()) {
				result //
						.withMarkPoint(rs.getBigDecimal(1).toString()) //
						.withTransactionId(rs.getString(2)) //
						.withMaxEventId(rs.getBigDecimal(3).toString()) //
						.withMaxSerialNo(rs.getInt(4)) //
				;
			}
			if (rs.last()) {
				rs.close();
				rs = null;
			}
			return result;
		} catch (SQLException e) {
			throw new SourceProcessException(e);
		}
	}
}
