package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(private val meterRegistry: MeterRegistry) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

        private const val THREADS = 64

        private const val QUEUE_CAPACITY = 64
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val incomingPayments = meterRegistry.counter("shop.payments", "stage", "incoming")
    private val rejectedPayments = meterRegistry.counter("shop.payments", "stage", "rejected")


    private val queueWaitTimer = meterRegistry.timer(
        "payment_stage_seconds",
        "component", "order-payer",
        "stage", "executor_queue"
    )

    private val createTimer = meterRegistry.timer(
        "payment_stage_seconds",
        "component", "order-payer",
        "stage", "es_create"
    )

    private val createFailures = meterRegistry.counter(
        "payment_stage_failures",
        "component", "order-payer",
        "stage", "es_create"
    )

    private val paymentExecutor: ThreadPoolExecutor = createExecutor()

    private fun createExecutor(): ThreadPoolExecutor {
        logger.warn("Payment submission executor: threads={}, queue={}", THREADS, QUEUE_CAPACITY)
        return ThreadPoolExecutor(
            THREADS,
            THREADS,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue<Runnable>(QUEUE_CAPACITY),
            NamedThreadFactory("payment-submission-executor"),
            RejectedExecutionHandler { _, _ ->
                rejectedPayments.increment()
                logger.warn("Payment rejected: payment queue is full")
                throw RejectedExecutionException("Payment queue is full")
            }
        ).also { ExecutorServiceMetrics(it, "payment-submission-executor", emptyList()).bindTo(meterRegistry) }
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val acceptedAt = System.currentTimeMillis()
        incomingPayments.increment()
        paymentExecutor.submit {
            queueWaitTimer.record(System.currentTimeMillis() - acceptedAt, TimeUnit.MILLISECONDS)
            submitPayment(paymentId, orderId, amount, deadline)
        }
        return acceptedAt
    }

    private fun submitPayment(paymentId: UUID, orderId: UUID, amount: Int, deadline: Long) {
        val createStart = System.currentTimeMillis()
        val createdEvent = try {
            paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
        } catch (e: Exception) {
            createFailures.increment()
            logger.error("Failed to create payment $paymentId for order $orderId, submission skipped", e)
            null
        } finally {
            createTimer.record(System.currentTimeMillis() - createStart, TimeUnit.MILLISECONDS)
        }
        if (createdEvent == null) return

        logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

        // Бюджет внешней оплаты начинается, когда воркер готов отправлять,
        // а не когда запрос ждал в локальной очереди исполнителя.
        val submissionStartedAt = System.currentTimeMillis()
        paymentService.submitPaymentRequest(paymentId, amount, submissionStartedAt, deadline)
    }
}
