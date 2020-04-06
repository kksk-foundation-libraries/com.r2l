package com.r2l;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class DataSourceFactory {
	private static final ConcurrentMap<String, DataSource> dataSource = new ConcurrentHashMap<>();

	public static DataSource get(String uri, String user, String password) {
		return dataSource.computeIfAbsent(uri, _uri -> _get(_uri, user, password));
	}

	private static DataSource _get(String uri, String user, String password) {
		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(uri, user, password);
		PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
		ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
		poolableConnectionFactory.setPool(connectionPool);
		poolableConnectionFactory.setDefaultAutoCommit(false);
		poolableConnectionFactory.setPoolStatements(true);
		return new PoolingDataSource<>(connectionPool);
	}
}
