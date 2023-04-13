package rabbitmq.rpc.server.service;

public interface RabbitMQServerService extends AutoCloseable {

    String run(String message);
}
