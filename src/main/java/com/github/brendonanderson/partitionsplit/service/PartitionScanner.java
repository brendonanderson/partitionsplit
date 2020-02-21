package com.github.brendonanderson.partitionsplit.service;

import com.github.brendonanderson.partitionsplit.dto.Product;
import com.github.brendonanderson.partitionsplit.vo.Range;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class PartitionScanner implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionScanner.class);
    private final KafkaConsumer<String, Product> kafkaConsumer;
    private final String topicName;
    private final int partition;
    private final long startOffset;
    private final long endOffset;

    public PartitionScanner(KafkaConsumer<String, Product> kafkaConsumer, String topicName, int partition, Range range) {
        this.kafkaConsumer = kafkaConsumer;
        this.topicName = topicName;
        this.partition = partition;
        this.startOffset = range.getStart();
        this.endOffset = range.getEnd();
    }

    @Override
    public void run() {
        LOG.info("Start {} End {}", startOffset, endOffset);
        TopicPartition topicPartition = new TopicPartition(topicName, partition);
        kafkaConsumer.assign(List.of(topicPartition));
        // move this consumer's offset to the beginning of it's range
        kafkaConsumer.seek(topicPartition, startOffset);
        long currentOffset = startOffset;

        // loop over the records until it reaches it's ending offset.  It may go beyond the ending
        // offset due to the batch size.
        while (currentOffset < endOffset) {
            ConsumerRecords<String, Product> records = kafkaConsumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, Product> record : records) {
                if (record.value().getModel() != null && record.value().getModel().equals("T65B")) {
                    // we found the record we were looking for!
                    LOG.info("Found record: {} - offset {}", record.value().getModel(), record.offset());
                }
            }
            currentOffset = kafkaConsumer.position(topicPartition);
        }
        kafkaConsumer.close();
    }
}
