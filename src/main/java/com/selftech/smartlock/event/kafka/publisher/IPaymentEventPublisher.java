package com.selftech.smartlock.event.kafka.publisher;

import com.selftech.kafka.event.publisher.IEventPublisherService;
import com.selftech.smartlock.models.entity.DeviceOperation;

/**
 * Payment Event Publisher Interface
 *
 * Contract for publishing payment-related events with both
 * reliable (Outbox Pattern) and best-effort async publishing options.
 *
 * Implementations must handle:
 * - Payment initiation events (payment process started)
 * - Payment completion events (successful payment)
 * - Payment failure events (payment rejected)
 * - Refund processing events (money returned to customer)
 *
 * Partition Key Strategy:
 * - Key: userId (ensures all events from same user go to same partition)
 * - Benefits: Ordered payment processing per user, fraud detection
 *
 * Usage in Managers:
 * - LockPaymentManager injects PaymentEventPublisherService directly (not interface)
 * - Call reliable methods within @Transactional boundaries (CRITICAL for financial)
 * - Call async methods only for non-financial notifications
 *
 * Event Types:
 * ═════════════════════════════════════════════════════════════════
 *
 * 1. PaymentInitiated (TIER 1 - Reliable)
 *    - Triggered: When payment process starts
 *    - Consumer: Payment gateway, financial reconciliation
 *    - Partition Key: userId
 *    - Critical: Must be persisted before charging customer
 *
 * 2. PaymentCompleted (TIER 1 - Reliable)
 *    - Triggered: When payment succeeds
 *    - Consumer: Accounting system, invoice generation
 *    - Partition Key: userId
 *    - Critical: Must be recorded for financial audit trail
 *
 * 3. PaymentFailed (TIER 1 - Reliable)
 *    - Triggered: When payment is rejected
 *    - Consumer: Retry logic, customer notification
 *    - Partition Key: userId
 *    - Critical: Must track failure reason for analysis
 *
 * 4. RefundProcessed (TIER 1 - Reliable)
 *    - Triggered: When refund is issued to customer
 *    - Consumer: Financial reconciliation, customer confirmation
 *    - Partition Key: userId
 *    - Critical: Must be persisted for fraud prevention
 *
 * ═════════════════════════════════════════════════════════════════
 *
 * Financial Data Protection:
 * - All payment events must use Outbox Pattern (TIER 1)
 * - Never use best-effort async for payment events
 * - Events contain sensitive financial information
 * - Requires audit trail and compliance logging
 *
 * @author Architecture Team - Phase 4.8
 * @since 2024 - Hybrid Architecture Implementation
 */
public interface IPaymentEventPublisher extends IEventPublisherService {

    /**
     * Publish payment initiation event asynchronously (TIER 2 - Best-Effort)
     *
     * NOTE: For financial events, use Reliable variant instead.
     * This method is provided for non-critical payment notifications only.
     *
     * @param operation DeviceOperation with payment details
     */
    void publishPaymentInitiatedEvent(DeviceOperation operation);

    /**
     * Publish payment initiation event with Outbox Pattern (TIER 1 - Reliable)
     *
     * Use when payment process starts and must be recorded.
     * Event persisted to outbox in same transaction as payment initiation.
     *
     * Critical Guarantees:
     * - Payment persisted before any charge to customer
     * - Exactly-once delivery ensures no double-charging
     * - Complete audit trail for compliance
     * - Can replay if downstream systems fail
     *
     * Must be called within @Transactional boundary.
     *
     * @param operation DeviceOperation that initiated payment
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishPaymentInitiatedEventReliable(DeviceOperation operation) throws Exception;

    /**
     * Publish payment completion event asynchronously (TIER 2 - Best-Effort)
     *
     * NOTE: For financial events, use Reliable variant instead.
     * This method is provided for non-critical payment notifications only.
     *
     * @param operation DeviceOperation that completed payment
     */
    void publishPaymentCompletedEvent(DeviceOperation operation);

    /**
     * Publish payment completion event with Outbox Pattern (TIER 1 - Reliable)
     *
     * Use when payment succeeds and must be recorded.
     * Event persisted to outbox in same transaction as payment completion.
     *
     * Critical Guarantees:
     * - Payment success recorded in database before returning
     * - Exactly-once delivery ensures proper accounting
     * - Complete audit trail for financial reconciliation
     * - Can replay if invoice generation fails
     *
     * Must be called within @Transactional boundary.
     *
     * @param operation DeviceOperation that completed payment
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishPaymentCompletedEventReliable(DeviceOperation operation) throws Exception;

    /**
     * Publish payment failure event asynchronously (TIER 2 - Best-Effort)
     *
     * NOTE: For critical failure tracking, use Reliable variant.
     *
     * @param operation DeviceOperation that failed
     * @param failureReason Reason for payment failure
     */
    void publishPaymentFailedEvent(DeviceOperation operation, String failureReason);

    /**
     * Publish payment failure event with Outbox Pattern (TIER 1 - Reliable)
     *
     * Use when payment is rejected and must be tracked.
     * Event persisted to outbox in same transaction as failure handling.
     *
     * Critical Guarantees:
     * - Failure reason persisted for analysis
     * - Exactly-once delivery prevents retry issues
     * - Complete audit trail for fraud detection
     * - Can replay if retry logic needs to re-evaluate
     *
     * Must be called within @Transactional boundary.
     *
     * @param operation DeviceOperation that failed
     * @param failureReason Reason for payment failure
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishPaymentFailedEventReliable(DeviceOperation operation, String failureReason) throws Exception;

    /**
     * Publish refund processing event asynchronously (TIER 2 - Best-Effort)
     *
     * NOTE: For critical refunds, use Reliable variant.
     *
     * @param operation DeviceOperation that was refunded
     */
    void publishRefundProcessedEvent(DeviceOperation operation);

    /**
     * Publish refund processing event with Outbox Pattern (TIER 1 - Reliable)
     *
     * Use when refund is issued to customer and must be recorded.
     * Event persisted to outbox in same transaction as refund processing.
     *
     * Critical Guarantees:
     * - Refund amount persisted before crediting customer
     * - Exactly-once delivery prevents duplicate refunds
     * - Complete audit trail for financial compliance
     * - Can replay if confirmation to customer fails
     *
     * Must be called within @Transactional boundary.
     *
     * @param operation DeviceOperation that was refunded
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishRefundProcessedEventReliable(DeviceOperation operation) throws Exception;
}
