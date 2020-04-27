package com.r2l;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.function.Function;

public class ResultSetIterable<T> implements Iterable<T> {
	private final Function<ResultSet, T> mapper;
	private final ResultSet rs;

	public ResultSetIterable(Function<ResultSet, T> mapper, ResultSet rs) {
		this.mapper = mapper;
		this.rs = rs;
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			@Override
			public boolean hasNext() {
				try {
					return !rs.last();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public T next() {
				try {
					if (rs.next()) {
						return mapper.apply(rs);
					}
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
				return null;
			}
		};
	}

}
