package com.r2l.source;

import java.sql.Connection;
import java.time.Duration;

import com.r2l.R2lSerSupplier;
import com.r2l.model.common.colf.OutboxCommandCollectResult;
import com.r2l.model.common.colf.OutboxEventCollectResult;
import com.r2l.model.common.colf.OutboxRecordCollectResult;
import com.r2l.source.func.OutboxCommandCollector;
import com.r2l.source.func.OutboxCommandFilter;
import com.r2l.source.func.OutboxCommandRemover;
import com.r2l.source.func.OutboxCommandSpliter;
import com.r2l.source.func.OutboxEventCollector;
import com.r2l.source.func.OutboxEventRemover;
import com.r2l.source.func.OutboxMarker;
import com.r2l.source.func.OutboxRecordCollector;
import com.r2l.source.func.OutboxRecordRemover;

import lombok.AllArgsConstructor;
import lombok.Data;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class Source {
	@Data
	@AllArgsConstructor
	public static class ReactorFlow {
		private Disposable markFlow;
		private Disposable collectFlow;
	}

	public ReactorFlow createReactorFlow() {
		R2lSerSupplier<Connection> connectionSupplier = new R2lSerSupplier<Connection>() {
			private static final long serialVersionUID = -8545369926257189764L;

			@Override
			public Connection get() {
				return null;
			}
		};
		Flux<Long> stream0 = Flux //
				.interval(Duration.ofSeconds(1L)) //
				.map(new OutboxMarker(connectionSupplier, 10000)) //
		;
		ConnectableFlux<OutboxCommandCollectResult> stream1 = Flux //
				.interval(Duration.ofSeconds(1L)) //
				.map(new OutboxCommandCollector(connectionSupplier, 1000)) //
				.filter(new OutboxCommandFilter()) //
				.publish() //
		;
		ConnectableFlux<OutboxEventCollectResult> stream2 = stream1 //
				.flatMapIterable(new OutboxCommandSpliter()) //
				.parallel(20) //
				.runOn(Schedulers.newParallel("OutboxEventCollectResult", 20)) //
				.map(new OutboxEventCollector(connectionSupplier)) //
				.sequential() //
				.publish() //
		;
		ConnectableFlux<OutboxRecordCollectResult> stream3 = stream2 //
				.parallel(20) //
				.runOn(Schedulers.newParallel("OutboxRecordCollector", 20)) //
				.map(new OutboxRecordCollector(connectionSupplier)) //
				.map(rec -> {
					// TODO Send to Kafka
					return rec;
				}) //
				.sequential() //
				.publish() //
		;
		stream3 //
				.parallel(20) //
				.runOn(Schedulers.newParallel("OutboxRecordRemover", 20)) //
				.map(new OutboxRecordRemover(connectionSupplier)) //
				.sequential() //
				.subscribe() //
		;
		stream2 //
				.parallel(20) //
				.runOn(Schedulers.newParallel("OutboxEventRemover", 20)) //
				.map(new OutboxEventRemover(connectionSupplier)) //
				.sequential() //
				.subscribe() //
		;
		stream1 //
				.map(new OutboxCommandRemover(connectionSupplier)) //
				.subscribe() //
		;
		stream3.connect();
		stream2.connect();
		return new ReactorFlow(stream0.subscribe(), stream1.connect());
	}
}
