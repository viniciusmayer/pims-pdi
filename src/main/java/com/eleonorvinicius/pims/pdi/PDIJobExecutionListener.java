package com.eleonorvinicius.pims.pdi;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FilenameUtils;
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

		String queueHost = null;
		String queueName = null;
		String pdiJobExtension = null;
		try {
			ConfiguracaoDAO configuracaoDAO = ConfiguracaoDAO.getInstance();
			queueHost = configuracaoDAO.getValor(ConfiguracaoEnum.QUEUE_HOST);
			queueName = configuracaoDAO.getValor(ConfiguracaoEnum.QUEUE_NAME);
			pdiJobExtension = configuracaoDAO.getValor(ConfiguracaoEnum.PDI_JOB_EXTENSION);
		} catch (SQLException sqlException) {
			logger.error("### ERROR: ");
			sqlException.printStackTrace();
			return;
		}

		List<String> listArgs = Arrays.asList(args);
		if (listArgs.isEmpty()) {
			logger.error("### ERROR: listArgs.isEmpty()");
			return;
		}

		File file = new File(listArgs.get(0));
		final List<String> pdiJobs = new ArrayList<String>();
		for (File _file : file.listFiles()) {
			if (FilenameUtils.getExtension(_file.getName()).equals(pdiJobExtension)) {
				pdiJobs.add(_file.getAbsolutePath());
			}
		}

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(queueHost);
		Connection connection = null;
		Channel channel = null;
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
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
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				logger.info("### BEGIN: DefaultConsumer.handleDelivery");
				Repository repository = null;
				try {
					KettleEnvironment.init();
				} catch (KettleException kettleException) {
					logger.error("### ERROR: KettleEnvironment.init()");
					kettleException.printStackTrace();
					return;
				}
				for (String pdiJob : pdiJobs) {
					JobMeta jobmeta;
					try {
						logger.info("### new JobMeta(): " + pdiJob);
						jobmeta = new JobMeta(pdiJob, null);
					} catch (KettleXMLException kettleXMLException) {
						logger.error("### ERROR: new JobMeta()");
						kettleXMLException.printStackTrace();
						return;
					}
					Job job = new Job(repository, jobmeta);
					job.start();
					job.waitUntilFinished();
					int errors = job.getErrors();
					if (errors > 0) {
						logger.info("### job.getErrors(): " + errors);
					}
				}
				logger.info("### END: DefaultConsumer.handleDelivery");
			}
		};
		try {
			channel.basicConsume(queueName, true, consumer);
		} catch (IOException ioException) {
			logger.error("### ERROR: channel.basicConsume()");
			ioException.printStackTrace();
			return;
		}
		logger.info("### END: PDIJobExecutionListener.main");
	}
}