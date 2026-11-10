package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong


class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
    private val expectedProcessingTimeMs: Long = 0L,
    private val maxAttempts: Int = 2,
    private val retryBackoffMs: Long = 100L,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        /** Резерв бюджета на запись результата в event store и доставку ответа клиенту. */
        private const val SUBMISSION_RESERVE_MS = 400L

        /** Минимальный бюджет попытки: меньше — гарантированный провал. */
        private const val MIN_ATTEMPT_BUDGET_MS = 50L

        /** Потолок времени одного HTTP-вызова: защита от зависшего соединения. */
        private const val CALL_TIMEOUT_CEILING_MS = 30_000L

        /** Вес предыдущей оценки в экспоненциальном сглаживании наблюдаемого времени ответа ПС. */
        private const val EWMA_PREVIOUS_WEIGHT = 4L

        /** Верхняя граница паузы между попытками. */
        private const val RETRY_BACKOFF_MAX_MS = 400L

        /** Пауза перед повтором, если ПС ответил 429 без `Retry-After`. */
        private const val DEFAULT_RETRY_AFTER_MS = 250L

        /** Пределы диспетчера OkHttp под in-flight, равный лимиту аккаунта. */
        private const val DISPATCHER_MAX_REQUESTS = 512
        private const val DISPATCHER_MAX_REQUESTS_PER_HOST = 256

        private const val CONNECT_TIMEOUT_MS = 2_000L
        private const val WRITE_TIMEOUT_MS = 5_000L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val client = OkHttpClient.Builder()
        .dispatcher(
            Dispatcher().apply {
                maxRequests = DISPATCHER_MAX_REQUESTS
                maxRequestsPerHost = DISPATCHER_MAX_REQUESTS_PER_HOST
            }
        )
        .connectTimeout(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .writeTimeout(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(
        properties.rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    private val parallelLimiter = Semaphore(properties.parallelRequests, true)

    private val rateWaitTimer = Timer.builder("payment_wait_seconds")
        .tag("account", accountName)
        .tag("stage", "rate")
        .register(meterRegistry)
    private val slotWaitTimer = Timer.builder("payment_wait_seconds")
        .tag("account", accountName)
        .tag("stage", "slot")
        .register(meterRegistry)
    private val processingTimer = Timer.builder("payment_processing_seconds")
        .tag("account", accountName)
        .register(meterRegistry)

    private val submissionLogTimer = Timer.builder("payment_stage_seconds")
        .tag("account", accountName)
        .tag("stage", "log_submission")
        .register(meterRegistry)

    private val processingLogTimer = Timer.builder("payment_stage_seconds")
        .tag("account", accountName)
        .tag("stage", "log_processing")
        .register(meterRegistry)

    private val e2eTimer = Timer.builder("payment_e2e_seconds")
        .tag("account", accountName)
        .register(meterRegistry)

    /** Наблюдаемое время ответа ПС (EWMA): по нему решается, влезает ли ещё одна попытка. */
    private val observedProcessingMs = AtomicLong(initialProcessingEstimate())

    private fun initialProcessingEstimate(): Long {
        if (expectedProcessingTimeMs > 0) return expectedProcessingTimeMs
        return properties.averageProcessingTime.toMillis()
    }

    private fun observeProcessingMs(durationMs: Long) {
        if (durationMs <= 0) return
        processingTimer.record(durationMs, TimeUnit.MILLISECONDS)

        while (true) {
            val previous = observedProcessingMs.get()
            val next = if (previous <= 0) {
                durationMs
            } else {
                (previous * EWMA_PREVIOUS_WEIGHT + durationMs) / (EWMA_PREVIOUS_WEIGHT + 1)
            }
            if (observedProcessingMs.compareAndSet(previous, next)) break
        }
    }

    /**
     * Остаток бюджета клиента за вычетом резерва на доставку результата.
     * Значение <= 0 означает, что отправлять (или повторять) уже поздно.
     */
    private fun remainingBudgetMs(deadline: Long): Long = deadline - now() - SUBMISSION_RESERVE_MS

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val startedAt = now()
        try {
            submitWithRetries(paymentId, amount, paymentStartedAt, deadline)
        } finally {
            e2eTimer.record(now() - startedAt, TimeUnit.MILLISECONDS)
        }
    }

    private fun submitWithRetries(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        // Платёж, которому не хватает бюджета ещё до попытки, не отправляем:
        // иначе деньги за попытку спишутся, а клиент всё равно увидит CLIENT_TIMEOUT.
        if (remainingBudgetMs(deadline) <= 0) {
            logTerminalFailure(
                paymentId,
                null,
                "Deadline exceeded before submission, request not sent",
                "deadline"
            )
            return
        }

        var attempt = 0
        while (true) {
            attempt++
            val transactionId = UUID.randomUUID()

            // Вне зависимости от исхода оплаты важно отметить, что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            val submissionLogStart = now()
            try {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to log submission for payment $paymentId, txId: $transactionId", e)
            } finally {
                submissionLogTimer.record(now() - submissionLogStart, TimeUnit.MILLISECONDS)
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId, attempt: $attempt")

            val rateWaitStart = now()
            rateLimiter.tickBlocking()
            rateWaitTimer.record(now() - rateWaitStart, TimeUnit.MILLISECONDS)

            val slotWaitStart = now()
            parallelLimiter.acquire()
            slotWaitTimer.record(now() - slotWaitStart, TimeUnit.MILLISECONDS)

            val outcome = try {
                callProvider(paymentId, amount, transactionId, deadline)
            } finally {
                parallelLimiter.release()
            }

            attemptCounter(attempt, if (outcome.success) "success" else "failed").increment()

            if (outcome.success) {
                logger.warn(
                    "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                            "succeeded: true, attempt: $attempt, message: ${outcome.reason}"
                )
                finish(paymentId, transactionId, true, outcome.reason, "success")
                return
            }

            logger.warn(
                "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                        "succeeded: false, attempt: $attempt, retriable: ${outcome.retriable}, message: ${outcome.reason}"
            )

            if (attempt >= maxAttempts || !outcome.retriable) {
                finish(paymentId, transactionId, false, outcome.reason, outcome.kind)
                return
            }

            // Пауза перед повтором: `Retry-After` от ПС либо линейный backoff.
            val backoffMs = outcome.retryAfterMs
                ?: minOf(retryBackoffMs * attempt, RETRY_BACKOFF_MAX_MS)

            // Повтор имеет смысл, только если остатка бюджета хватает ещё на один вызов ПС.
            if (remainingBudgetMs(deadline) <= observedProcessingMs.get() + backoffMs) {
                logger.warn(
                    "[$accountName] No budget for retry for payment $paymentId: " +
                            "remaining=${remainingBudgetMs(deadline)} ms, needed=${observedProcessingMs.get() + backoffMs} ms"
                )
                finish(paymentId, transactionId, false, "${outcome.reason} (no budget for retry)", outcome.kind)
                return
            }

            retryCounter(outcome.kind).increment()
            logger.warn("[$accountName] Retrying payment $paymentId after ${backoffMs} ms, reason: ${outcome.kind}")
            if (backoffMs > 0) Thread.sleep(backoffMs)
        }
    }

    /**
     * Один вызов платёжной системы. Время вызова ограничено остатком дедлайна клиента,
     * чтобы зависшее соединение не съело бюджет и не удерживало слот.
     */
    private fun callProvider(paymentId: UUID, amount: Int, transactionId: UUID, deadline: Long): AttemptOutcome {
        val callBudgetMs = remainingBudgetMs(deadline)
            .coerceIn(MIN_ATTEMPT_BUDGET_MS, CALL_TIMEOUT_CEILING_MS)

        val request = Request.Builder().run {
            url(
                "http://$paymentProviderHostPort/external/process" +
                        "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                        "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            )
            post(emptyBody)
        }.build()

        val sentAt = now()
        return try {
            client.newBuilder()
                .callTimeout(callBudgetMs, TimeUnit.MILLISECONDS)
                .build()
                .newCall(request)
                .execute()
                .use { response ->
                    observeProcessingMs(now() - sentAt)
                    interpret(response)
                }
        } catch (e: SocketTimeoutException) {
            observeProcessingMs(now() - sentAt)
            AttemptOutcome(false, "Request timeout.", retriable = true, kind = "timeout")
        } catch (e: InterruptedIOException) {
            observeProcessingMs(now() - sentAt)
            AttemptOutcome(false, "Request timeout (call deadline).", retriable = true, kind = "timeout")
        } catch (e: IOException) {
            observeProcessingMs(now() - sentAt)
            AttemptOutcome(false, e.message ?: e.javaClass.simpleName, retriable = true, kind = "io")
        } catch (e: Exception) {
            observeProcessingMs(now() - sentAt)
            logger.error("[$accountName] Unexpected payment failure for txId: $transactionId, payment: $paymentId", e)
            AttemptOutcome(false, e.message ?: e.javaClass.simpleName, retriable = false, kind = "unexpected")
        }
    }

    private fun interpret(response: Response): AttemptOutcome {
        val bodyText = try {
            response.body?.string()
        } catch (e: Exception) {
            null
        }

        val parsed = bodyText?.let { raw ->
            runCatching { mapper.readValue(raw, ExternalSysResponse::class.java) }.getOrNull()
        }

        if (parsed != null && response.isSuccessful) {
            if (parsed.result) {
                return AttemptOutcome(
                    success = true,
                    reason = parsed.message,
                    retriable = false,
                    kind = "success",
                )
            }

            return AttemptOutcome(
                success = false,
                reason = parsed.message,
                retriable = true,
                kind = "declined",
                retryAfterMs = 0L,
            )
        }

        val retriable = isRetriableStatus(response.code)
        val reason = parsed?.message ?: "HTTP ${response.code}: ${bodyText?.take(200)}"
        return AttemptOutcome(
            success = false,
            reason = reason,
            retriable = retriable,
            kind = kindOfStatus(response.code),
            retryAfterMs = if (response.code == 429) parseRetryAfterMs(response) else null,
        )
    }

    private fun isRetriableStatus(status: Int): Boolean =
        status == 429 || status == 500 || status == 502 || status == 503 || status == 504

    private fun kindOfStatus(status: Int): String = when {
        status == 429 -> "http_429"
        status in 500..599 -> "http_5xx"
        else -> "http_$status"
    }

    /** `Retry-After` отдаётся в секундах; ограничиваем паузу разумным диапазоном. */
    private fun parseRetryAfterMs(response: Response): Long {
        val header = response.header("Retry-After")
        val seconds = header?.trim()?.toLongOrNull()
        val result = if (seconds != null) {
            seconds * 1000L
        } else {
            DEFAULT_RETRY_AFTER_MS
        }
        return result.coerceIn(0L, RETRY_BACKOFF_MAX_MS * 10)
    }

    private fun finish(paymentId: UUID, transactionId: UUID, success: Boolean, reason: String?, label: String) {
        outcomeCounter(success, label).increment()
        val processingLogStart = now()
        try {
            // Результат пишется ровно один раз — по финальному исходу платежа.
            paymentESService.update(paymentId) {
                it.logProcessing(success, now(), transactionId, reason = reason)
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Failed to store payment result for payment $paymentId, txId: $transactionId", e)
        } finally {
            processingLogTimer.record(now() - processingLogStart, TimeUnit.MILLISECONDS)
        }
    }

    /** Терминальный исход без отправки запроса (бюджет исчерпан ещё до попытки). */
    private fun logTerminalFailure(paymentId: UUID, transactionId: UUID?, reason: String, label: String) {
        outcomeCounter(false, label).increment()
        logger.error("[$accountName] Payment $paymentId dropped: $reason")
        try {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = reason)
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Failed to store drop result for payment $paymentId", e)
        }
    }

    private fun attemptCounter(attempt: Int, outcome: String): Counter =
        meterRegistry.counter(
            "payment_attempts",
            "account", accountName,
            "attempt", attempt.toString(),
            "outcome", outcome,
        )

    private fun retryCounter(reason: String): Counter =
        meterRegistry.counter("payment_retry", "account", accountName, "reason", reason)

    private fun outcomeCounter(success: Boolean, label: String): Counter =
        meterRegistry.counter(
            "shop_payment_finished",
            "account", accountName,
            "outcome", if (success) "SUCCESS" else "FAIL",
            "failReason", label,
        )

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private data class AttemptOutcome(
        val success: Boolean,
        val reason: String?,
        val retriable: Boolean,
        val kind: String,
        val retryAfterMs: Long? = null,
    )
}

fun now() = System.currentTimeMillis()
