package com.selftech.smartlock.event.kafka.publisher;

import com.selftech.kafka.event.publisher.IEventPublisherService;
import com.selftech.smartlock.models.entity.BoxOperation;
import com.selftech.smartlock.models.entity.LockBox;

/**
 * Lock Box Event Publisher Interface
 *
 * Contract for publishing smart lock box-related events with both
 * reliable (Outbox Pattern) and best-effort async publishing options.
 *
 * Implementations must handle:
 * - Box creation events (new smart lock box deployed)
 * - Box status change events (active, inactive, maintenance)
 * - Box door open events (security-critical, anomaly detection)
 * - Box weight change events (anomaly detection triggers)
 * - Generic box operation events (comprehensive audit trail)
 *
 * Partition Key Strategy:
 * - Key: boxCode (ensures all events from same box go to same partition)
 * - Benefits: Ordered event processing per box, deterministic routing
 *
 * Usage in Managers:
 * - LockBoxOperationManager injects LockBoxEventPublisherService directly (not interface)
 * - LockBoxManager injects LockBoxEventPublisherService directly (not interface)
 * - Call reliable methods within @Transactional boundaries
 * - Call async methods for non-critical notifications
 *
 * Event Types:
 * ═════════════════════════════════════════════════════════════════
 *
 * 1. BoxCreated (TIER 1 - Reliable)
 *    - Triggered: When new smart lock box is created/deployed
 *    - Consumer: Box inventory system, analytics
 *    - Partition Key: boxCode
 *
 * 2. BoxStatusChanged (TIER 1 - Reliable)
 *    - Triggered: When box operational status changes
 *    - Consumer: Status tracking, maintenance scheduling
 *    - Partition Key: boxCode
 *
 * 3. BoxOpened (TIER 1 - Reliable)
 *    - Triggered: When box door is opened (security event)
 *    - Consumer: Security monitoring, audit logging
 *    - Partition Key: boxCode
 *
 * 4. BoxWeightChanged (TIER 1 - Reliable)
 *    - Triggered: When weight changes beyond threshold
 *    - Consumer: Anomaly detection, fraud prevention
 *    - Partition Key: boxCode
 *
 * 5. BoxOperationPerformed (TIER 1 - Reliable)
 *    - Triggered: Generic box operation events
 *    - Consumer: Complete operation audit trail
 *    - Partition Key: boxCode
 *
 * ═════════════════════════════════════════════════════════════════
 *
 * @author Architecture Team - Phase 4.8
 * @since 2024 - Hybrid Architecture Implementation
 */
public interface ILockBoxEventPublisher extends IEventPublisherService {

    /**
     * Publish box creation event asynchronously (TIER 2 - Best-Effort)
     *
     * @param box LockBox entity representing created box
     */
    void publishBoxCreatedEvent(LockBox box);

    /**
     * Publish box creation event with Outbox Pattern (TIER 1 - Reliable)
     *
     * @param box LockBox entity representing created box
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishBoxCreatedEventReliable(LockBox box) throws Exception;

    /**
     * Publish box status change event asynchronously (TIER 2 - Best-Effort)
     *
     * @param box LockBox entity with new status
     * @param previousStatus Previous box status (as string)
     */
    void publishBoxStatusChangedEvent(LockBox box, String previousStatus);

    /**
     * Publish box status change event with Outbox Pattern (TIER 1 - Reliable)
     *
     * @param box LockBox entity with new status
     * @param previousStatus Previous box status (as string)
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishBoxStatusChangedEventReliable(LockBox box, String previousStatus) throws Exception;

    /**
     * Publish box door open event asynchronously (TIER 2 - Best-Effort)
     *
     * Security-related but non-critical for primary operation.
     *
     * @param box LockBox entity with door status
     */
    void publishBoxOpenedEvent(LockBox box);

    /**
     * Publish box door open event with Outbox Pattern (TIER 1 - Reliable)
     *
     * Security-critical event that must be reliably recorded.
     *
     * @param box LockBox entity with door status
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishBoxOpenedEventReliable(LockBox box) throws Exception;

    /**
     * Publish box weight change event asynchronously (TIER 2 - Best-Effort)
     *
     * @param box LockBox entity with new weight
     * @param previousWeight Previous weight value
     */
    void publishBoxWeightChangedEvent(LockBox box, Double previousWeight);

    /**
     * Publish box weight change event with Outbox Pattern (TIER 1 - Reliable)
     *
     * Weight changes trigger anomaly detection systems.
     *
     * @param box LockBox entity with new weight
     * @param previousWeight Previous weight value
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishBoxWeightChangedEventReliable(LockBox box, Double previousWeight) throws Exception;

    /**
     * Publish generic box operation event asynchronously (TIER 2 - Best-Effort)
     *
     * @param boxOp BoxOperation entity
     */
    void publishBoxOperationEvent(BoxOperation boxOp);

    /**
     * Publish generic box operation event with Outbox Pattern (TIER 1 - Reliable)
     *
     * @param boxOp BoxOperation entity
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishBoxOperationEventReliable(BoxOperation boxOp) throws Exception;
}
