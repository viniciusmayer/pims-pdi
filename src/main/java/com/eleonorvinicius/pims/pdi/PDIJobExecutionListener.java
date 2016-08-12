package com.eleonorvinicius.pims.pdi;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
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

	public static void main(String[] args) {

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST);
		Connection connection = null;
		Channel channel = null;
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		} catch (TimeoutException e1) {
			e1.printStackTrace();
			return;
		}

		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String file = PDI_JOB;
				Repository repository = null;
				try {
					KettleEnvironment.init();
					JobMeta jobmeta = new JobMeta(file, null);
					Job job = new Job(repository, jobmeta);
					job.start();
					job.waitUntilFinished();
					if (job.getErrors() > 0) {
						System.out.println("Error Executing Job");
					}
				} catch (KettleException e) {
					e.printStackTrace();
				}
			}
		};
		try {
			channel.basicConsume(QUEUE_NAME, true, consumer);
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
	}
}