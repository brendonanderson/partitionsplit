package com.github.brendonanderson.partitionsplit;

import com.github.brendonanderson.partitionsplit.dto.Product;
import com.github.brendonanderson.partitionsplit.service.ThreadManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

// Uncomment the below line when you want to load sample data (and comment this line in PartitionSplitApplication)
//@SpringBootApplication
public class LoadDataApplication implements CommandLineRunner {

	private final KafkaTemplate<String, Product> kafkaTemplate;
	private final String topicName;

	public LoadDataApplication(KafkaTemplate<String, Product> kafkaTemplate, @Value("${app.topicName}") String topicName) {
		this.kafkaTemplate = kafkaTemplate;
		this.topicName = topicName;
	}

	public static void main(String[] args) {
		SpringApplication.run(LoadDataApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		for (int i = 0; i < 1_000_000; i++) {
			Product product = new Product();
			product.setSerialNumber((long)i);
			String model = "YT-1300";
			if (i % 100_000 == 1000) {
				model = "T65B";
			}
			product.setModel(model);
			product.setDescription(String.format("Model %s Serial %d", model, i));

			kafkaTemplate.send(topicName, "key-" + i, product);
		}
	}
}
