package rabbitmq.rpc.client.service;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface RabbitMQClientService extends AutoCloseable {

    String send(String message) throws IOException, InterruptedException, TimeoutException;
}
