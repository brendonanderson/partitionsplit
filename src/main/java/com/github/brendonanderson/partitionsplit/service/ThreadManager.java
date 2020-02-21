package com.github.brendonanderson.partitionsplit.service;

import com.github.brendonanderson.partitionsplit.dto.Product;
import com.github.brendonanderson.partitionsplit.vo.Range;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class ThreadManager {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadManager.class);
    private final List<KafkaConsumer<String, Product>> kafkaConsumers;
    private final String topicName;
    private final int partition;

    public ThreadManager(List<KafkaConsumer<String, Product>> kafkaConsumers,
                         @Value("${app.topicName}") String topicName,
                         @Value("${app.partition}") int partition) {
        this.kafkaConsumers = kafkaConsumers;
        this.topicName = topicName;
        this.partition = partition;
    }

    public void process() {
        TopicPartition topicPartition = new TopicPartition(topicName, partition);
        // grab the first kafka consumer and use it to calculate offsets
        KafkaConsumer<String, Product> kafkaConsumer = kafkaConsumers.get(0);
        kafkaConsumer.assign(List.of(topicPartition));

        // find the beginning and ending offsets to calculate total records on partition
        kafkaConsumer.seekToBeginning(List.of(topicPartition));
        long startOffset = kafkaConsumer.position(topicPartition);
        kafkaConsumer.seekToEnd(List.of(topicPartition));
        long endOffset = kafkaConsumer.position(topicPartition);

        // figure out the offset ranges for each thread
        List<Range> ranges = calculateRanges(startOffset, endOffset, kafkaConsumers.size());

        // create the threads used to process the data in the ranges
        List<Thread> threads = IntStream.range(0, kafkaConsumers.size())
                .mapToObj(i -> new Thread(new PartitionScanner(kafkaConsumers.get(i), topicName, partition, ranges.get(i))))
                .collect(Collectors.toList());

        // start the threads
        threads.forEach(Thread::start);

        // wait for the threads to finish
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                LOG.error("Interrupted", e);
            }
        }
    }

    public List<Range> calculateRanges(long startOffset, long endOffset, int numberOfSlices) {
        long sliceSize = (endOffset - startOffset) / numberOfSlices;
        List<Range> ranges = IntStream.range(0, numberOfSlices)
                .mapToObj(i -> new Range(startOffset + (i * sliceSize), startOffset + (i * sliceSize) + sliceSize - 1))
                .collect(Collectors.toList());

        //make sure last range includes the endOffset
        ranges.set(numberOfSlices - 1, new Range(ranges.get(numberOfSlices - 1).getStart(), endOffset));
        return ranges;
    }
}
