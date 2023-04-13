package rabbitmq.rpc.client.service.impl;

import com.rabbitmq.client.*;
import lombok.extern.java.Log;
import org.springframework.stereotype.Service;
import rabbitmq.rpc.client.service.RabbitMQClientService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Log
@Service
public class RabbitMQClientServiceImpl implements RabbitMQClientService {

    private static final String RPC_QUEUE_NAME = "q.rpc_queue";

    private final Connection connection;

    public RabbitMQClientServiceImpl() throws IOException, TimeoutException {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connection = connectionFactory.newConnection();
    }

    @Override
    public String send(final String message) throws IOException, InterruptedException, TimeoutException {
        final Channel channel = connection.createChannel();
        final String exclusiveQueueName = channel.queueDeclare("", false, true, true, null).getQueue();

        final String correlationId = UUID.randomUUID().toString();
        log.info(String.format("RPC call correlation id: %s", correlationId));

        final AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                .correlationId(correlationId)
                .replyTo(exclusiveQueueName)
                .build();

        channel.basicPublish("", RPC_QUEUE_NAME, properties, message.getBytes(StandardCharsets.UTF_8));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        final DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            final String result = new String(delivery.getBody(), StandardCharsets.UTF_8);
            if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                log.info(String.format("RPC correlation id: %s completed with result: %s", delivery.getProperties().getCorrelationId(), result));
                response.offer(result);
            }
        };

        channel.basicConsume(exclusiveQueueName, true, deliverCallback, consumerTag -> { });

        final String result = response.poll(5, TimeUnit.SECONDS);
        log.info(String.format("Returning correlation id: %s result: %s", correlationId, result));

        channel.close();
        return result;
    }

    @Override
    public void close() throws Exception {
        this.connection.close();
    }
}
