package com.github.brendonanderson.partitionsplit;

import com.github.brendonanderson.partitionsplit.service.ThreadManager;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Uncomment the below line when you want to run the scanner program (and comment the below line in LoadDataApplication)
@SpringBootApplication
public class PartitionSplitApplication implements CommandLineRunner {

	private final ThreadManager threadManager;

	public PartitionSplitApplication(ThreadManager threadManager) {
		this.threadManager = threadManager;
	}

	public static void main(String[] args) {
		SpringApplication.run(PartitionSplitApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		this.threadManager.process();
	}
}
