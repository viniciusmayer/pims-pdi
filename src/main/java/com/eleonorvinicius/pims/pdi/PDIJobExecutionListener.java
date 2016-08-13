package com.eleonorvinicius.pims.pdi;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class PDIJobExecutionListener {

	private static final String PDI_JOB = "src/main/resources/analiseJob.kjb";
	private static final String HOST = "localhost";
	private static final String QUEUE_NAME = "pims";
	private static final Logger logger = LogManager.getLogger(PDIJobExecutionListener.class.getName());

	public static void main(String[] args) {
		logger.info("### BEGIN: main");

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		Connection connection = null;
		Channel channel = null;
		try {
			connection = factory.newConnection();
			logger.info("### factory.newConnection()");
			channel = connection.createChannel();
			logger.info("### connection.createChannel()");
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			logger.info("### channel.queueDeclare()");
		} catch (IOException ioException) {
			logger.error("### ERROR: ");
			ioException.printStackTrace();
			return;
		} catch (TimeoutException timeoutException) {
			logger.error("### ERROR: ");
			timeoutException.printStackTrace();
			return;
		}

		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				logger.info("### BEGIN: DefaultConsumer.handleDelivery");
				Repository repository = null;
				try {
					KettleEnvironment.init();
					logger.info("### KettleEnvironment.init()");
				} catch (KettleException e) {
					logger.error("### ERROR: KettleEnvironment.init()");
					e.printStackTrace();
					return;
				}
				JobMeta jobmeta;
				try {
					jobmeta = new JobMeta(PDI_JOB, null);
					logger.info("### new JobMeta()");
				} catch (KettleXMLException e) {
					logger.error("### ERROR: new JobMeta()");
					e.printStackTrace();
					return;
				}
				Job job = new Job(repository, jobmeta);
				logger.info("### new Job()");
				job.start();
				logger.info("### job.start()");
				job.waitUntilFinished();
				logger.info("### job.waitUntilFinished()");
				if (job.getErrors() > 0) {
					logger.info("### job.getErrors()");
				}
			}
		};
		try {
			channel.basicConsume(QUEUE_NAME, true, consumer);
			logger.info("### channel.basicConsume()");
		} catch (IOException ioException) {
			logger.error("### ERROR: channel.basicConsume()");
			ioException.printStackTrace();
			return;
		}
		logger.info("### END");
	}
}