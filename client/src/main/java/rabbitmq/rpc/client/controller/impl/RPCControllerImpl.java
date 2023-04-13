package rabbitmq.rpc.client.controller.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import rabbitmq.rpc.client.controller.RPCController;
import rabbitmq.rpc.client.service.RabbitMQClientService;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@RestController
public class RPCControllerImpl implements RPCController {

    private final RabbitMQClientService rabbitMQClientService;

    @Autowired
    public RPCControllerImpl(final RabbitMQClientService rabbitMQClientService) {
        this.rabbitMQClientService = rabbitMQClientService;
    }

    @GetMapping("send")
    public ResponseEntity<String> send(final String message) throws IOException, InterruptedException, TimeoutException {
        final String result = rabbitMQClientService.send(message);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }
}
