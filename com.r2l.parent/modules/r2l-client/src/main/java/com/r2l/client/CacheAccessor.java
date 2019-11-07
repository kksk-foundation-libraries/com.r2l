package com.r2l.client;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import com.r2l.model.IndexValue;
import com.r2l.model.PrimaryKeys;
import com.r2l.schema.Binary;
import com.r2l.schema.IndexKey;
import com.r2l.schema.PrimaryKey;
import com.r2l.schema.RdbRecord;

public abstract class CacheAccessor<IK extends IndexKey, PK extends PrimaryKey, V extends RdbRecord> {
	protected abstract PK newPrimaryKeyInstance();

	protected abstract V newValueInstance();

	private final IgniteCache<Binary, byte[]> masterCache;
	private final IgniteCache<Binary, byte[]> indexCache;

	public CacheAccessor(Ignite cluster, String masterCacheName, String indexCacheName) {
		this(cluster, cluster, masterCacheName, indexCacheName);
	}

	public CacheAccessor(Ignite masterCluster, Ignite indexCluster, String masterCacheName, String indexCacheName) {
		this.masterCache = masterCluster.cache(masterCacheName);
		this.indexCache = indexCluster.cache(indexCacheName);
	}

	private static class IndexGetEntryProcessor implements EntryProcessor<Binary, byte[], byte[]> {
		@Override
		public byte[] process(MutableEntry<Binary, byte[]> entry, Object... arguments) throws EntryProcessorException {
			if (!entry.exists()) {
				return null;
			}
			long businessDate = (long) arguments[0];
			IndexValue indexValue = new IndexValue().unmarshal(entry.getValue());
			byte[][] result = Arrays //
					.asList(indexValue.historicalData)//
					.stream() //
					.filter(histricalData -> histricalData.getBegin() <= businessDate && businessDate < histricalData.getEnd()) //
					.map(historicalData -> historicalData.getPrimaryKey()) //
					.collect(Collectors.toList()) //
					.toArray(new byte[][] {});
			if (result == null || result.length == 0) {
				return null;
			}
			PrimaryKeys primaryKeys = new PrimaryKeys().withData(result);
			return primaryKeys.marshal();
		}

	}

	private static final IndexGetEntryProcessor INDEX_GET_ENTRY_PROCESSOR = new IndexGetEntryProcessor();
	private static final byte[] EMPTY = {};

	@SuppressWarnings("unchecked")
	public List<V> get(IK key, long businessDate) {
		Binary binIndexKey = Binary.of(key);
		byte[] binIndexValue = indexCache.invoke(binIndexKey, INDEX_GET_ENTRY_PROCESSOR, businessDate);
		if (binIndexValue == null) {
			return null;
		}
		PrimaryKeys primaryKeys = new PrimaryKeys().unmarshal(binIndexValue);
		return Arrays //
				.asList(primaryKeys.data) //
				.stream() //
				.map(binPrimaryKey -> (PK) newPrimaryKeyInstance().unmarshal(binPrimaryKey)) //
				.parallel() //
				.map(primaryKey -> Binary.of(primaryKey)) //
				.map(binPrimaryKey -> {
					byte[] result = masterCache.get(binPrimaryKey);
					if (result == null)
						return EMPTY;
					return result;
				}) //
				.filter(binData -> binData != EMPTY) //
				.map(binData -> (V) newValueInstance().unmarshal(binData)) //
				.sequential() //
				.collect(Collectors.toList()) //
		;
	}
}
