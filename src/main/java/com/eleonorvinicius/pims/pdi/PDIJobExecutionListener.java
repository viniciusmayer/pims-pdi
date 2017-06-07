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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;

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
		Channel channel = null;
		try {
			channel = factory.newConnection().createChannel();
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

		Consumer consumer = new PDIConsumer(channel, pdiJobs);
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