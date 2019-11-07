package com.r2l.schema;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class Binary {
	private byte[] data;

	private Binary() {
	}

	private Binary set(byte[] data) {
		this.data = data;
		return this;
	}

	public static Binary of(ColferObject object) {
		return new Binary().set(object.marshal());
	}

	public <T extends ColferObject> T mapTo(T target) {
		return target.unmarshal(data);
	}
}
