package com.r2l;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.r2l.model.common.colf.OutboxRecord;

public interface RecordCollector {
	String eventTarget();

	PreparedStatement createQuery(Connection connection);

	OutboxRecord map(ResultSet rs);

	PreparedStatement createDeleteStatement(Connection connection);
}
