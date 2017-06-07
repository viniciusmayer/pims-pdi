package com.eleonorvinicius.pims.pdi;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class PDIConsumer extends DefaultConsumer {

	private static final Logger logger = LogManager.getLogger(PDIConsumer.class.getName());
	private List<String> pdiJobs;

	public PDIConsumer(Channel channel, List<String> pdiJobs) {
		super(channel);
		this.pdiJobs = pdiJobs;
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
			throws IOException {
		logger.info("### BEGIN: DefaultConsumer.handleDelivery");
		try {
			KettleEnvironment.init();
		} catch (KettleException kettleException) {
			logger.error("### ERROR: KettleEnvironment.init()");
			kettleException.printStackTrace();
			return;
		}
		for (String pdiJob : this.pdiJobs) {
			JobMeta jobmeta;
			try {
				logger.info("### new JobMeta(): " + pdiJob);
				jobmeta = new JobMeta(pdiJob, null);
			} catch (KettleXMLException kettleXMLException) {
				logger.error("### ERROR: new JobMeta()");
				kettleXMLException.printStackTrace();
				return;
			}
			Job job = new Job(null, jobmeta);
			job.start();
			job.waitUntilFinished();
			int errors = job.getErrors();
			if (errors > 0) {
				logger.info("### job.getErrors(): " + errors);
			}
		}
		logger.info("### END: DefaultConsumer.handleDelivery");
	}
}
