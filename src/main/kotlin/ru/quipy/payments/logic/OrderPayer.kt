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

    // Back pressure: at most 16 + 24 = 40 accepted payments at once. acc-5 finishes ~1 payment/s
    // (5 parallel requests / 4.9 s), so the last of them is done in ~40 s, well before the 60 s deadline.
    // When the queue is full, the payment is rejected and the client gets 429 (see APIController).
    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(24),
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