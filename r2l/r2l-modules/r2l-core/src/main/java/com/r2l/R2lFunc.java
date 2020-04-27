package com.r2l;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class R2lFunc<InputT, OutputT> implements SerializableFunction<InputT, OutputT> {
	private static final long serialVersionUID = -178695864376797305L;

	public static <InputT, OutputT> SerializableFunction<InputT, OutputT> of(R2lSerFunc<InputT, OutputT> function) {
		return new R2lFunc<>(new R2lSerSupplier<R2lSerFunc<InputT, OutputT>>() {
			private static final long serialVersionUID = -2776844055826891426L;
			private final R2lSerFunc<InputT, OutputT> _function = function;

			@Override
			public R2lSerFunc<InputT, OutputT> get() {
				return _function;
			}
		});
	}

	private R2lSerFunc<InputT, OutputT> function;
	private final R2lSerSupplier<R2lSerFunc<InputT, OutputT>> supplier;

	private R2lFunc(R2lSerSupplier<R2lSerFunc<InputT, OutputT>> supplier) {
		this.supplier = supplier;
	}

	@Override
	public OutputT apply(InputT input) {
		if (function == null) {
			function = supplier.get();
		}
		return function.apply(input);
	}

}
