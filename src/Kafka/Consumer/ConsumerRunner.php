<?php

declare(strict_types=1);

namespace SimPod\KafkaBundle\Kafka\Consumer;

use Psr\Log\LoggerInterface;
use RdKafka\KafkaConsumer;
use SimPod\KafkaBundle\Kafka\Brokers;
use SimPod\KafkaBundle\Kafka\Consumer\Exception\IncompatibleStatus;
use SimPod\KafkaBundle\Kafka\Consumer\Exception\RebalancingFailed;
use function microtime;
use const RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS;
use const RD_KAFKA_RESP_ERR__PARTITION_EOF;
use const RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS;
use const RD_KAFKA_RESP_ERR__TIMED_OUT;
use const RD_KAFKA_RESP_ERR_NO_ERROR;

final class ConsumerRunner
{
    /** @var Brokers */
    private $brokers;

    /** @var int */
    private $processedMessageCount = 0;

    /** @var Configuration */
    private $configuration;

    /** @var null|LoggerInterface */
    private $logger;

    public function __construct(Brokers $brokers, ?LoggerInterface $logger = null)
    {
        $this->brokers = $brokers;
        $this->logger  = $logger;
    }

    public function run(Consumer $consumer) : void
    {
        $this->configuration = $consumer->getConfiguration();

        $kafkaConfiguration = $this->configuration->get();
        $kafkaConfiguration->set(ConfigConstants::METADATA_BROKER_LIST, $this->brokers->getList());

        $kafkaConfiguration->setRebalanceCb(
            function (KafkaConsumer $kafka, int $err, ?array $partitions = null) : void {
                switch ($err) {
                    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                        $this->tryLog('Assigning partitions', $partitions);
                        $kafka->assign($partitions);
                        break;

                    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                        $this->tryLog('Revoking partitions', $partitions);
                        $kafka->assign(null);
                        break;

                    default:
                        throw RebalancingFailed::new($err);
                }
            }
        );

        $kafkaConsumer = new KafkaConsumer($kafkaConfiguration);
        $kafkaConsumer->subscribe($consumer->getTopics());

        $this->startConsuming($kafkaConsumer, $consumer);
    }

    /**
     * @param mixed[]|null $context
     */
    private function tryLog(string $message, ?array $context = []) : void
    {
        if ($this->logger === null) {
            return;
        }

        $this->logger->info($message, $context ?? []);
    }

    private function startConsuming(KafkaConsumer $kafkaConsumer, Consumer $consumer) : void
    {
        $startTime = microtime(true);

        while (true) {
            if (! $this->shouldContinue($startTime)) {
                break;
            }

            $message = $kafkaConsumer->consume(120 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $consumer->consume($message, $kafkaConsumer);
                    $this->processedMessageCount++;
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $this->tryLog('No more messages; will wait for more');
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $this->tryLog('Timed out');
                    break;
                default:
                    throw IncompatibleStatus::fromMessage($message);
            }
        }
    }

    private function shouldContinue(float $startTime) : bool
    {
        return $this->hasAnyMessagesLeft() && $this->hasAnyTimeLeft($startTime);
    }

    private function hasAnyMessagesLeft() : bool
    {
        $maxMessages = $this->configuration->getMaxMessages();

        return $maxMessages === null || $maxMessages > $this->processedMessageCount;
    }

    private function hasAnyTimeLeft(float $startTime) : bool
    {
        $maxSeconds = $this->configuration->getMaxSeconds();

        return $maxSeconds === null || microtime(true) < $startTime + $maxSeconds;
    }
}
