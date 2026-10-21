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
class OrderPayer(meterRegistry: MeterRegistry) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val incomingPayments = meterRegistry.counter("shop.payments", "stage", "incoming")
    private val rejectedPayments = meterRegistry.counter("shop.payments", "stage", "rejected")

    // acc-18 takes 110 payments/s, a payment lasts 0..2 s (1 s on average), the deadline is 3 s.
    // Threads: ~110 payments are in flight at once (110/s * 1 s), 140 leaves room for slow ones.
    // Buffer: the spare threads (waiting for the rate limiter) plus the queue hold ~100 payments = ~0.9 s of work,
    // so a buffered payment still has >= 2 s left for the provider call.
    // When the buffer is full, the payment is rejected and the client gets 429 (see APIController).
    private val paymentExecutor = ThreadPoolExecutor(
        140,
        140,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(70),
        NamedThreadFactory("payment-submission-executor"),
        RejectedExecutionHandler { _, _ ->
            rejectedPayments.increment()
            logger.warn("Payment rejected: payment queue is full")
            throw RejectedExecutionException("Payment queue is full")
        }
    ).also { ExecutorServiceMetrics(it, "payment-submission-executor", emptyList()).bindTo(meterRegistry) }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        incomingPayments.increment()
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}