package rabbitmq.rpc.server.service.impl;

import com.rabbitmq.client.*;
import lombok.extern.java.Log;
import org.springframework.stereotype.Service;
import rabbitmq.rpc.server.service.RabbitMQServerService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@Log
@Service
public class RabbitMQServerServiceImpl implements RabbitMQServerService {

    private static final String RPC_QUEUE_NAME = "q.rpc_queue";

    private final Connection connection;

    private final Channel channel;

    public RabbitMQServerServiceImpl() throws IOException, TimeoutException {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        channel.queuePurge(RPC_QUEUE_NAME);
        channel.basicQos(1);
        setup();
    }

    private void setup() throws IOException {
        final DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            final String correlationId = delivery.getProperties().getCorrelationId();
            final AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                    .correlationId(correlationId)
                    .build();

            final String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            final String replyToQueue = delivery.getProperties().getReplyTo();
            log.info(String.format("RPC server received a message: %s with correlation id: %s, for reply to: %s", message, correlationId, replyToQueue));

            final String result = run(message);
            channel.basicPublish("", replyToQueue, properties, result.getBytes(StandardCharsets.UTF_8));
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> {
        }));
    }

    @Override
    public String run(final String message) {
        return String.format("Message processed: %s", message);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
