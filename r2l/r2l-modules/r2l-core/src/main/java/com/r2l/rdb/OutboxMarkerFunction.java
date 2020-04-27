package com.r2l.rdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.r2l.DataSourceFactory;

public class OutboxMarkerFunction extends SimpleFunction<Long, Long> {
	/** serialVersionUID */
	private static final long serialVersionUID = 8460095539301599045L;

	private static final Logger LOG = LoggerFactory.getLogger(OutboxMarkerFunction.class);
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
	public static final Long ERRORED = 0L;
	private final String uri;
	private final String user;
	private final String password;
	private final int updateCount;
	Connection conn;
	PreparedStatement ps;

	public OutboxMarkerFunction(String uri, String user, String password, int updateCount) {
		this.uri = uri;
		this.user = user;
		this.password = password;
		this.updateCount = updateCount;
	}

	@Override
	public Long apply(Long input) {
		try {
			if (conn == null) {
				conn = DataSourceFactory.get(uri, user, password).getConnection();
			}
			if (ps == null) {
				ps = conn.prepareStatement(SQL_MARKING);
			} else {
				ps.clearParameters();
			}
			ps.setInt(1, updateCount);
			long updated = ps.executeUpdate();
			return updated;
		} catch (SQLException e) {
			LOG.error("sql error.", e);
			throw new RuntimeException(e);
		} finally {
			if (conn != null) {
				try {
					conn.commit();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}