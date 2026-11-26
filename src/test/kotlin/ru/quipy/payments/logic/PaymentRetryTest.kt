package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.withSettings
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.InetSocketAddress
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentRetryTest {
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    private val registry = SimpleMeterRegistry()
    private val calls = AtomicInteger()
    private val workers = Executors.newCachedThreadPool()
    private val store = mock<EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>>(
        withSettings().defaultAnswer { null },
    )
    private val adapter = PaymentExternalSystemAdapterImpl(
        PaymentAccountProperties("shop", "acc-7", 4, 20, 1, Duration.ofMillis(50), true),
        store, "127.0.0.1:${server.address.port}", "test", registry,
        maxAttempts = 3, retryBackoffMs = 0, attemptTimeoutMs = 500,
    )

    @AfterEach
    fun close() {
        server.stop(0)
        workers.shutdownNow()
        registry.close()
    }

    private fun runPayment(vararg statuses: Int, budgetMs: Long = 6000, slowFirst: Boolean = false) {
        val retryArrived = CountDownLatch(1)
        server.executor = workers
        server.createContext("/external/process") { request ->
            val index = calls.getAndIncrement()
            if (slowFirst && index == 0) {
                retryArrived.await(4, TimeUnit.SECONDS)
                request.close()
                return@createContext
            }
            retryArrived.countDown()
            val status = statuses[index]
            val body = """{"transactionId":"tx","paymentId":"id","result":true}""".toByteArray()
            request.sendResponseHeaders(status, body.size.toLong())
            request.responseBody.use { it.write(body) }
        }
        server.start()
        val start = System.currentTimeMillis()
        adapter.performPaymentAsync(UUID.randomUUID(), 100, start, start + budgetMs)
    }

    private fun checkCounts(sent: Int, repeats: Int, recovered: Int) {
        assertEquals(sent, calls.get())
        assertEquals(repeats.toDouble(), registry.find("payment_retry").counters().sumOf { it.count() })
        assertEquals(recovered.toDouble(), registry.get("payment_recovered").counter().count())
    }

    @Test
    fun countsOnlyExtraCallsAndOneRecovery() {
        runPayment(503, 502, 200)
        checkCounts(3, 2, 1)
    }

    @Test
    fun stopsAtAttemptLimit() {
        runPayment(503, 503, 503)
        checkCounts(3, 2, 0)
    }

    @Test
    fun skipsPermanentFailures() {
        runPayment(400)
        checkCounts(1, 0, 0)
    }

    @Test
    fun skipsExpiredPayments() {
        runPayment(budgetMs = 0)
        checkCounts(0, 0, 0)
    }

    @Test
    fun retriesSlowCallWithinPaymentDeadline() {
        runPayment(200, 200, slowFirst = true)
        checkCounts(2, 1, 1)
        assertEquals(1.0, registry.get("payment_retry").tag("reason", "timeout").counter().count())
    }
}
