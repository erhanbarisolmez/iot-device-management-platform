package com.selftech.smartlock.config;

import java.util.concurrent.Executor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Async event publishing yapılandırması
 *
 * Manager'lardan event publisher'lara gönderilen yayınları
 * asynchronous olarak (background thread'lerde) çalıştırmak için.
 *
 * Bu, API response'unu engellememektedir.
 * Event publishing Kafka'ya gitmesi sırasında, request thread'i
 * hemen response döner.
 */
@Configuration
@EnableAsync(proxyTargetClass = true)
public class AsyncConfig {

    /**
     * Async event publishing için TaskExecutor bean'i
     *
     * Performance Profile:
     * - CorePoolSize: 10 (normal yük'te 10 thread çalışır)
     * - MaxPoolSize: 50 (peak yük'te 50'ye kadar scale eder)
     * - QueueCapacity: 100 (queue'de bekleyebilecek max task)
     *
     * Behavior:
     * - 1-10 task: core threads (immediate execution)
     * - 11-50 task: spawn new threads (up to max)
     * - 51-150 task: 50 threads + queue
     * - 150+: RejectedExecutionException (failure handling)
     */
    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // Normal yük: 10 thread çalışsın
        executor.setCorePoolSize(10);

        // Peak yük: 50 thread'e kadar scale et
        executor.setMaxPoolSize(50);

        // Core+Max dolu ise, 100 task kuyruğa gir
        executor.setQueueCapacity(100);

        // Debugging için thread name'ine prefix ekle
        executor.setThreadNamePrefix("async-event-");

        // Boş kalan thread'ler 60 saniye sonra kapatılsın
        executor.setKeepAliveSeconds(60);

        // Shutdown sırasında devam eden task'lar bitmesini bekle (graceful shutdown)
        executor.setWaitForTasksToCompleteOnShutdown(true);

        // Maksimum 10 saniye bekle, sonra force shutdown et
        executor.setAwaitTerminationSeconds(10);

        // Bean'i initialize et
        executor.initialize();

        return executor;
    }
}
