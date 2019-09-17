
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
// import io.debezium.connector.sqlserver.SqlServerConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.BasicConfigurator;


class TestEmbedded {


	final static Logger logger = LoggerFactory.getLogger("mylogger");

/**
	TestEmbedded() {
		Configuration config = Configuration.create()
				// begin engine properties //
				.with("name", "db2-connector")
				.with("connector.class", "io.debezium.connector.db2.Db2Connector")
				.with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
				.with("offset.storage.file.filename", "/tmp/path/to/storage/offset.dat")
				.with("offset.flush.interval.ms", 60000)
				// begin connector properties //
				.with("database.hostname", "db2cdc.urb.zc2.ibm.com")
				.with("database.port", 50000)
				.with("database.user", "db2inst1")
				.with("database.password", "db2DEVadmin")
				.with("database.server.name", "DB2CDC")
				.with("database.dbname", "SAMPLE")
				.with("database.cdcschema", "ASNCDC")
				.with("server.id", 85744)
				.with("server.name", "my-app-connector")
				.with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
				.with("database.history.file.filename", "/tmp/path/to/storage/dbhistory.dat")
				.build();
 **/
		TestEmbedded() {
			Configuration config = Configuration.create()
					// begin engine properties //
					.with("name", "db2-connector")
					.with("connector.class", "io.debezium.connector.db2.Db2Connector")
					.with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
					.with("offset.storage.file.filename", "/tmp/path/to/storage/offset.dat")
					.with("offset.flush.interval.ms", 60000)
					// begin connector properties //
					.with("database.hostname", "skipnix.zurich.ibm.com")
					.with("database.port", 50000)
					.with("database.user", "db2inst1")
					.with("database.password", "pass29Aug")
					.with("database.server.name", "TESTDB")
					.with("database.dbname", "TESTDB")
					.with("database.cdcschema", "ASNCDC")
					.with("server.id", 85744)
					.with("server.name", "my-app-connector")
					.with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
					.with("database.history.file.filename", "/tmp/path/to/storage/dbhistory.dat")
					.build();

		// Create the engine with this configuration ...
		EmbeddedEngine engine = EmbeddedEngine.create()
				.using(config)
				.notifying(this::handleEvent)
				.build();

		// Run the engine asynchronously ...
		ExecutorService executor = Executors.newSingleThreadExecutor();
		System.out.println("Before Executor");
		executor.execute(engine);
		try {
			executor.awaitTermination(160,TimeUnit.SECONDS);
		}
		catch(Exception e) {
           	e.printStackTrace();
		}
		System.out.println("After Executor");
		engine.stop();
	}

	void handleEvent(SourceRecord rec) {
		logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!" + rec.toString());
		logger.warn("TOPIC == " + rec.topic());
		logger.warn("VALUE == " + rec.value());
		logger.warn("KEY == " + rec.key());
		logger.warn("SOURCE OFFSET == " + rec.sourceOffset());
	}



	public static void main(String args[]) {
		System.out.println("Hello");

		//MySqlConnector x = new MySqlConnector();
		BasicConfigurator.configure();

		logger.info("+ Test Embedded");
		TestEmbedded t = new TestEmbedded();
		logger.info("- TestEmbedded");
	}

}
