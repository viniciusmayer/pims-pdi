package com.eleonorvinicius.pims.pdi;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
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

	private static final Logger logger = LogManager.getLogger(PDIJobExecutionListener.class.getName());

	public static void main(String[] args) {
		logger.info("### BEGIN: PDIJobExecutionListener.main");

		String queueHost = "localhost";
		String queueName = "pims";
		try {
			logger.info("### ConfiguracaoDAO.getInstance()");
			ConfiguracaoDAO configuracaoDAO = ConfiguracaoDAO.getInstance();
			logger.info("### configuracaoDAO.getValor():QUEUE_HOST");
			queueHost = configuracaoDAO.getValor("QUEUE_HOST");
			logger.info("### configuracaoDAO.getValor():QUEUE_NAME");
			queueName = configuracaoDAO.getValor("QUEUE_NAME");
		} catch (SQLException sqlException) {
			logger.error("### ERROR: ");
			sqlException.printStackTrace();
			return;
		}
		
		List<String> listArgs = Arrays.asList(args);
		if (listArgs.isEmpty()){
			logger.error("### ERROR: listArgs.isEmpty()");
			return;
		}
		
		final String pdiJob = listArgs.get(0);
		if (pdiJob == null || pdiJob.isEmpty()){
			logger.error("### ERROR: pdiJob.isEmpty()");
			return;
		}

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(queueHost);
		Connection connection = null;
		Channel channel = null;
		try {
			logger.info("### factory.newConnection()");
			connection = factory.newConnection();
			logger.info("### connection.createChannel()");
			channel = connection.createChannel();
			logger.info("### channel.queueDeclare()");
			channel.queueDeclare(queueName, false, false, false, null);
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
					logger.info("### KettleEnvironment.init()");
					KettleEnvironment.init();
				} catch (KettleException kettleException) {
					logger.error("### ERROR: KettleEnvironment.init()");
					kettleException.printStackTrace();
					return;
				}
				JobMeta jobmeta;
				try {
					logger.info("### new JobMeta()");
					jobmeta = new JobMeta(pdiJob, null);
				} catch (KettleXMLException kettleXMLException) {
					logger.error("### ERROR: new JobMeta()");
					kettleXMLException.printStackTrace();
					return;
				}
				logger.info("### new Job()");
				Job job = new Job(repository, jobmeta);
				logger.info("### job.start()");
				job.start();
				logger.info("### job.waitUntilFinished()");
				job.waitUntilFinished();
				if (job.getErrors() > 0) {
					logger.info("### job.getErrors()");
				}
				logger.info("### END: DefaultConsumer.handleDelivery");
			}
		};
		try {
			logger.info("### channel.basicConsume()");
			channel.basicConsume(queueName, true, consumer);
		} catch (IOException ioException) {
			logger.error("### ERROR: channel.basicConsume()");
			ioException.printStackTrace();
			return;
		}
		logger.info("### END: PDIJobExecutionListener.main");
	}
}