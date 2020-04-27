package com.r2l.rdb;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithFailures.ExceptionElement;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutboxMarker {
	private static final Logger LOG = LoggerFactory.getLogger(OutboxMarker.class);

	public static void main(String[] args) {
		Options options = new Options();
		options.addRequiredOption("c", "config", true, "configuration file path.");
		try {
			CommandLine cl = new DefaultParser().parse(options, args);
			String configFilePath = cl.getOptionValue("c");
			try (InputStream in = new FileInputStream(configFilePath)) {
				Properties prop = new Properties();
				prop.load(in);
				OutboxMarker outboxMarker = new OutboxMarker(prop);
				outboxMarker.run();
			} catch (Exception e) {
				e.printStackTrace();
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("java ... " + OutboxMarker.class.getName() + " -c {path to config file}", options, true);
			}
		} catch (ParseException e) {
			e.printStackTrace();
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java ... " + OutboxMarker.class.getName() + " -c {path to config file}", options, true);
		}
	}

	private final String uri;
	private final String user;
	private final String password;

	private final long updateInterval;

	private final int updateCount;

	private OutboxMarker(Properties properties) {
		uri = properties.getProperty("jdbc.connection.uri");
		user = properties.getProperty("jdbc.user");
		password = properties.getProperty("jdbc.password");
		updateInterval = Long.parseLong(properties.getProperty("rdb.mark.interval", "1000"));
		updateCount = Integer.parseInt(properties.getProperty("rdb.mark.updates", "1000"));
	}

	private static final DateTimeFormatter FMT_yyyyMMddHH = DateTimeFormatter.ofPattern("yyyyMMddHH").withLocale(Locale.JAPAN).withZone(ZoneId.systemDefault());
	private static final DateTimeFormatter FMT_yyyyMMddHHmmss = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withLocale(Locale.JAPAN).withZone(ZoneId.systemDefault());

	private void run() {
		Pipeline pipeline = Pipeline.create();
		Result<PCollection<Long>, Long> process = //
				pipeline //
						.apply("start stream", GenerateSequence.from(0L).withRate(1L, Duration.millis(updateInterval))) //
						.apply("mark", //
								MapElements //
										.via(new OutboxMarkerFunction(uri, user, password, updateCount)) //
										.exceptionsVia(new InferableFunction<ExceptionElement<Long>, Long>() {
											/** serialVersionUID */
											private static final long serialVersionUID = -8833592360977149240L;

											@Override
											public Long apply(ExceptionElement<Long> input) throws Exception {
												LOG.error("error!@" + input.element(), input.exception());
												return OutboxMarkerFunction.ERRORED;
											}
										}) //
						) //
		;
		PCollection<Long> stream = process.output();
		stream //
				.apply("filter no op.", Filter.greaterThan(0l)) //
				.apply("sum processed.", Sum.longsGlobally()) //
				.apply("log format", MapElements.via(new SimpleFunction<Long, String>() {
					/** serialVersionUID */
					private static final long serialVersionUID = -8833592360977149240L;

					@Override
					public String apply(Long input) {
						return FMT_yyyyMMddHHmmss.format(Instant.now()) + ":" + String.valueOf(input.longValue());
					}
				})) //
				.apply("write to file.", TextIO.write().to(new ValueProvider<String>() {
					/** serialVersionUID */
					private static final long serialVersionUID = 1L;

					@Override
					public boolean isAccessible() {
						return true;
					}

					@Override
					public String get() {
						return FMT_yyyyMMddHH.format(Instant.now());
					}
				})) //
		;
		pipeline.run().waitUntilFinish();
	}
}
