package com.r2l;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class PipelineCock {
	private static final Timer TIMER = new Timer("zookeeper path check thread.", true);
	private static final ConcurrentMap<String, CuratorFramework> CURATOR_REPO = new ConcurrentHashMap<>();
	private static final ConcurrentMap<String, String> SCHEDULED_PATHS = new ConcurrentHashMap<>();

	public static Open open(String zkServers, String zkPath, long interval) {
		return new Open(zkServers, zkPath, interval);
	}

	public static class Open extends PTransform<PBegin, PCollection<Integer>> {
		/** serialVersionUID */
		private static final long serialVersionUID = -2149788216805250459L;

		private final AtomicBoolean stopped = new AtomicBoolean(false);

		private final String zkServers;
		private final String zkPath;
		private final long interval;

		public Open(String zkServers, String zkPath, long interval) {
			this.zkServers = zkServers;
			this.zkPath = zkPath;
			this.interval = interval;
			try {
				curator().setData().forPath(zkPath, "1".getBytes());
			} catch (Exception e) {
				stopped.set(true);
			}
		}

		private CuratorFramework curator() {
			return CURATOR_REPO //
					.computeIfAbsent(zkServers, _zkServers -> {
						return CuratorFrameworkFactory //
								.builder() //
								.connectString(_zkServers) //
								.retryPolicy(new ExponentialBackoffRetry(100, 10)) //
								.build() //
						;
					}) //
			;
		}

		@Override
		public PCollection<Integer> expand(PBegin input) {
			SCHEDULED_PATHS.computeIfAbsent(zkServers + ":" + zkPath, _key -> {
				TIMER.schedule(new TimerTask() {
					@Override
					public void run() {
						try {
							stopped.compareAndSet(false, curator().getData().forPath(zkPath) == null);
						} catch (Exception e) {
							stopped.set(true);
						}
					}
				}, interval, interval);
				return _key;
			});
			return input.apply("check exists zookeeper node", Create.ofProvider(new ValueProvider<Integer>() {
				/** serialVersionUID */
				private static final long serialVersionUID = -8929970543369756093L;

				@Override
				public Integer get() {
					return !stopped.get() ? 1 : 0;
				}

				@Override
				public boolean isAccessible() {
					return !stopped.get();
				}
			}, VarIntCoder.of()));
		}
	}

}
