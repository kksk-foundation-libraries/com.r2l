package com.r2l.source.func;

import java.io.Serializable;
import java.sql.ResultSet;

public interface OutboxRecordConverter extends Serializable {
	String query();

	String deleteStatement();

	byte[] toBytes(ResultSet rs);
}
