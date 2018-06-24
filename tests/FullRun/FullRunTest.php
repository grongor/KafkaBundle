<?php

declare(strict_types=1);

namespace SimPod\KafkaBundle\Tests\FullRun;

use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\TopicConf;
use SimPod\KafkaBundle\Kafka\Consumer\Configuration;
use SimPod\KafkaBundle\Kafka\Consumer\Consumer;
use SimPod\KafkaBundle\Kafka\Consumer\ConsumerRunner;
use SimPod\KafkaBundle\Kafka\Producer;
use SimPod\KafkaBundle\Tests\KafkaTestCase;

final class FullRunTest extends KafkaTestCase implements Consumer
{
    private const PAYLOAD    = 'Tasty, chilled pudding is best flavored with juicy lime.';
    private const TEST_TOPIC = 'test-topic';

    public function testRun() : void
    {
        $container = $this->createYamlBundleTestContainer();

        $testProducer = new TestProducer($container->get(Producer::class));
        $testProducer->produce(self::PAYLOAD, self::TEST_TOPIC);

        /** @var ConsumerRunner $consumerRunner */
        $consumerRunner = $container->get(ConsumerRunner::class);
        $consumerRunner->run($this);
    }

    public function consume(Message $kafkaMessage, KafkaConsumer $kafkaConsumer) : void
    {
        self::assertSame(self::PAYLOAD, $kafkaMessage->payload);
    }

    public function getConfiguration() : Configuration
    {
        $configuration = new Configuration($this->getGroupId(), 1);

        $topicConf = new TopicConf();
        $topicConf->set('auto.offset.reset', 'earliest');
        $configuration->setDefaultTopicConf($topicConf);

        return $configuration;
    }

    public function getGroupId() : string
    {
        return 'test';
    }

    /**
     * @return string[]
     */
    public function getTopics() : array
    {
        return [self::TEST_TOPIC];
    }
}
