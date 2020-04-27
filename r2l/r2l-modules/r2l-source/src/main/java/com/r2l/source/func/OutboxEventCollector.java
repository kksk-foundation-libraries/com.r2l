package com.r2l.source.func;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.r2l.R2lSerFunc;
import com.r2l.R2lSerSupplier;
import com.r2l.model.common.colf.OutboxEventCollectResult;
import com.r2l.source.SourceProcessException;

public class OutboxEventCollector implements R2lSerFunc<OutboxEventCollectResult, OutboxEventCollectResult> {
	private static final long serialVersionUID = -6385801067013943511L;

	private static final String SQL_COLLECT_EVENT = String.join("\n" //
			, "SELECT" //
			, "  OBE.EVENT_ID" //
			, ", OBE.SEQUENCE_NO" //
			, ", OBE.EVENT_TARGET" //
			, ", OBE.ACTION" //
			, "FROM" //
			, "  OUTBOX_EVENT OBE" //
			, "WHERE" //
			, "    OBE.TRANSACTION_ID = ?" //
			, "AND OBE.SERIAL_NO = ?" //
			, "ORDER BY" //
			, "  OBE.SERIAL_NO" //
			, ", OBE.EVENT_ID" //
	);

	private final R2lSerSupplier<Connection> connectionSupplier;
	private Connection connection;
	private PreparedStatement ps;
	private ResultSet rs;

	public OutboxEventCollector(R2lSerSupplier<Connection> connectionSupplier) {
		this.connectionSupplier = connectionSupplier;
	}

	@Override
	public OutboxEventCollectResult apply(OutboxEventCollectResult t) {
		try {
			if (rs == null) {
				if (connection == null) {
					connection = connectionSupplier.get();
					connection.setAutoCommit(true);
				}
				if (ps == null) {
					ps = connection.prepareStatement(SQL_COLLECT_EVENT, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				} else {
					ps.clearParameters();
				}
				ps.setString(1, t.getTransactionId());
				ps.setInt(2, t.getSerialNo());
				rs = ps.executeQuery();
			}
			OutboxEventCollectResult res = new OutboxEventCollectResult() //
					.withMarkPoint(t.getMarkPoint()) //
					.withTransactionId(t.getTransactionId()) //
					.withMaxEventId(t.getMaxEventId()) //
					.withMaxSerialNo(t.getMaxSerialNo()) //
					.withEventId(rs.getBigDecimal(1).toString()) //
					.withSerialNo(rs.getInt(2)) //
					.withEventTarget(rs.getString(3)) //
					.withAction(rs.getByte(4)) //
			;
			rs.close();
			rs = null;
			return res;
		} catch (SQLException e) {
			throw new SourceProcessException(e);
		}
	}

}
