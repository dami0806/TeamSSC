package com.sparta.teamssc.rabbitmq;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class RabbitMQConfig {

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final SlackNotificationService slackNotificationService;

    @Value("${spring.rabbitmq.host:localhost}")
    private String rabbitHost;

    // 일반 큐
    public static final String QUEUE_NAME = "chat-queue";
    // 데드레터 큐
    public static final String DEAD_LETTER_QUEUE = "chat-queue-dlq";

    public static final String EXCHANGE_NAME = "chat-exchange";

    //교환기
    public static final String DEAD_LETTER_EXCHANGE = "chat-queue-dlx";

    // 일반 큐 설정
    @Bean
    public Queue queue() {
        return QueueBuilder.durable(QUEUE_NAME)
                .deadLetterExchange(DEAD_LETTER_EXCHANGE)
                .deadLetterRoutingKey(DEAD_LETTER_QUEUE)
                .ttl(360000) // 6분
                .build();
    }

    // 데드레터 큐 설정
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DEAD_LETTER_QUEUE).build();
    }

    // 데드레터 교환기 설정
    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(DEAD_LETTER_EXCHANGE);
    }

    // 일반 교환기 설정
    @Bean
    public DirectExchange exchange() {
        return new DirectExchange(EXCHANGE_NAME);
    }


    // 데드레터 큐와 데드레터 교환기 바인딩
    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue())
                .to(deadLetterExchange())
                .with(DEAD_LETTER_QUEUE);
    }

    // 일반 큐와 교환기 바인딩
    @Bean
    public Binding binding() {
        return BindingBuilder.bind(queue())
                .to(exchange())
                .with(QUEUE_NAME);
    }

    // JSON 메시지 변환기 설정
    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // @RabbitListener 컨테이너 설정
    // defaultRequeueRejected=false: 예외 발생 시 requeue하지 않고 DLQ로 이동
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setDefaultRequeueRejected(false);
        factory.setMessageConverter(messageConverter());
        return factory;
    }

    // RabbitTemplate에 메시지 변환기 및 Publisher Confirms 설정
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(messageConverter());

        // Exchange → Queue 라우팅 실패 시 콜백 (바인딩 깨짐, 큐 삭제 등)
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setReturnsCallback(returned -> {
            log.error("메시지 라우팅 실패 - exchange: {}, routingKey: {}, replyText: {}",
                    returned.getExchange(), returned.getRoutingKey(), returned.getReplyText());
            slackNotificationService.sendNotification(
                    "RabbitMQ 메시지 라우팅 실패 - routingKey: " + returned.getRoutingKey()
                    + ", 사유: " + returned.getReplyText()
            );
        });

        // Broker가 메시지를 받았는지 확인 (publish ACK/NACK)
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.error("RabbitMQ publish NACK - cause: {}", cause);
                slackNotificationService.sendNotification("RabbitMQ publish 실패(NACK) - " + cause);
            }
        });

        return rabbitTemplate;
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost(rabbitHost);
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        connectionFactory.setVirtualHost("/");
        connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        connectionFactory.setPublisherReturns(true);
        connectionFactory.addConnectionListener(new ConnectionListener() {
            @Override
            public void onCreate(com.rabbitmq.client.Connection connection) {
                log.info("RabbitMQ 연결 성공");
                CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker("rabbitmq-consumer");
                if (cb.getState() == CircuitBreaker.State.OPEN) {
                    cb.transitionToHalfOpenState();
                    log.info("RabbitMQ 재연결 감지 - CircuitBreaker HALF_OPEN 전환");
                }
            }

            @Override
            public void onClose(com.rabbitmq.client.ShutdownSignalException signal) {
                log.error("RabbitMQ 연결 끊김: {}", signal.getMessage());
                CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker("rabbitmq-consumer");
                cb.transitionToOpenState();
                log.warn("CircuitBreaker OPEN 전환");
                slackNotificationService.sendNotification("RabbitMQ Broker 연결 끊김 감지 - CircuitBreaker OPEN");
            }
        });
        return connectionFactory;
    }

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }
}