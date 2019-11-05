package com.r2l;

import com.r2l.schema.IndexKey;
import com.r2l.schema.PrimaryKey;
import com.r2l.schema.RdbRecord;

public abstract class RdbRecordDescriptor<RdbRecordType extends RdbRecord> {
	public final int indexCount;
	private final PrimaryKeyTranslator<RdbRecordType> primaryKeyTranslator;
	private final IndexKeyTranslator<RdbRecordType, ? extends IndexKey>[] indexKeyTranslators;

	@SafeVarargs
	protected RdbRecordDescriptor(PrimaryKeyTranslator<RdbRecordType> primaryKeyTranslator, IndexKeyTranslator<RdbRecordType, ? extends IndexKey>... indexKeyTranslators) {
		this.primaryKeyTranslator = primaryKeyTranslator;
		this.indexKeyTranslators = indexKeyTranslators;
		this.indexCount = indexKeyTranslators.length;
	}

	public PrimaryKey primaryKey(RdbRecordType rdbRecord) {
		return primaryKeyTranslator.apply(rdbRecord);
	}

	@SuppressWarnings("unchecked")
	public <T extends IndexKey> T indexKey(RdbRecordType rdbRecord, int indexNumber) {
		return (T) indexKeyTranslators[indexNumber].apply(rdbRecord);
	}

	public abstract RdbRecordType createRdbRecord();
}
