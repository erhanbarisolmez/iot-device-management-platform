package com.selftech.smartlock.event.kafka.publisher;

import com.selftech.kafka.event.publisher.IEventPublisherService;
import com.selftech.smartlock.models.entity.DeviceOperation;
import com.selftech.smartlock.models.entity.LockDevice;

/**
 * Lock Device Event Publisher Interface
 *
 * Contract for publishing smart lock device-related events with both
 * reliable (Outbox Pattern) and best-effort async publishing options.
 *
 * Implementations must handle:
 * - Device creation events (new device deployed)
 * - Device status change events (online, offline, maintenance)
 * - Device assignment events (assigned to box or user)
 * - Device operation events (lock, unlock, status updates)
 *
 * Partition Key Strategy:
 * - Key: deviceCode (ensures all events from same device go to same partition)
 * - Benefits: Ordered device event processing, deterministic routing
 *
 * Usage in Managers:
 * - LockDeviceManager injects LockDeviceEventPublisherService directly (not interface)
 * - LockBoxOperationManager injects LockDeviceEventPublisherService directly (not interface)
 * - Call reliable methods within @Transactional boundaries
 * - Call async methods for non-critical notifications
 *
 * Event Types:
 * ═════════════════════════════════════════════════════════════════
 *
 * 1. DeviceCreated (TIER 1 - Reliable)
 *    - Triggered: When new device is registered
 *    - Consumer: Device inventory system
 *    - Partition Key: deviceCode
 *
 * 2. DeviceStatusChanged (TIER 2 - Async)
 *    - Triggered: When device status changes
 *    - Consumer: Status monitoring, analytics
 *    - Partition Key: deviceCode
 *    - Note: Can use async as status is ephemeral
 *
 * 3. DeviceOperation (TIER 1 - Reliable)
 *    - Triggered: Lock/unlock/assignment operations
 *    - Consumer: Audit trail, operation history
 *    - Partition Key: deviceCode
 *
 * ═════════════════════════════════════════════════════════════════
 *
 * @author Architecture Team - Phase 4.8
 * @since 2024 - Hybrid Architecture Implementation
 */
public interface ILockDeviceEventPublisher extends IEventPublisherService {

    /**
     * Publish device creation event asynchronously (TIER 2 - Best-Effort)
     *
     * @param device LockDevice entity representing created device
     */
    void publishDeviceCreatedEvent(LockDevice device);

    /**
     * Publish device creation event with Outbox Pattern (TIER 1 - Reliable)
     *
     * @param device LockDevice entity representing created device
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishDeviceCreatedEventReliable(LockDevice device) throws Exception;

    /**
     * Publish device status change event asynchronously (TIER 2 - Best-Effort)
     *
     * @param device LockDevice entity with new status
     * @param previousStatus Previous device status (as string)
     */
    void publishDeviceStatusChangedEvent(LockDevice device, String previousStatus);

    /**
     * Publish device status change event with Outbox Pattern (TIER 1 - Reliable)
     *
     * @param device LockDevice entity with new status
     * @param previousStatus Previous device status (as string)
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishDeviceStatusChangedEventReliable(LockDevice device, String previousStatus) throws Exception;

    /**
     * Publish device operation event asynchronously (TIER 2 - Best-Effort)
     *
     * @param deviceOperation DeviceOperation entity
     */
    void publishDeviceOperationEvent(DeviceOperation deviceOperation);

    /**
     * Publish device operation event with Outbox Pattern (TIER 1 - Reliable)
     *
     * @param deviceOperation DeviceOperation entity
     * @throws Exception if outbox save fails (transaction will rollback)
     */
    void publishDeviceOperationEventReliable(DeviceOperation deviceOperation) throws Exception;
}
