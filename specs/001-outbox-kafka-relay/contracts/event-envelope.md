# Contract: Kafka Event Envelope & Topic Mapping

---

## 1. JSON Event Envelope Specification

Each Kafka message value produced by the relay worker MUST strictly conform to the following JSON structure:

```json
{
  "eventId": "a5e78370-13d8-4f81-9b1b-60a5e848df74",
  "eventType": "directory.employee.created",
  "eventVersion": 1,
  "tenantCode": "tenant-enterprise-a",
  "occurredAt": "2026-09-28T10:00:00Z",
  "producer": "hros-directory-service",
  "correlationId": null,
  "causationId": null,
  "traceId": null,
  "payload": {
    "employeeId": "e1432f74-d4b9-4448-b4b1-6a1005a30364",
    "email": "employee@example.com",
    "department": "Engineering"
  }
}
```

---

## 2. Topic Resolution Rules

Given `event_type` of the format `<domain>.<aggregate>.<action>` (e.g. `directory.employee.created`):
1. Default topic name: `<domain>.<aggregate>` (e.g., `directory.employee`).
2. If `topic_prefix` is configured and not already prefixed: `<topic_prefix>.<domain>.<aggregate>`.

Examples:

| Event Type | Topic Prefix | Resolved Kafka Topic |
|------------|--------------|----------------------|
| `directory.employee.created` | `""` | `directory.employee` |
| `directory.employee.updated` | `""` | `directory.employee` |
| `setting.company.updated` | `""` | `setting.company` |
| `access.role.assigned` | `""` | `access.role` |
| `directory.employee.created` | `hros` | `hros.directory.employee` |

---

## 3. Kafka Message Headers & Keys

- **Message Key**: UTF-8 bytes of `aggregate_id` (or `tenant_code + ":" + aggregate_id` if tenant-keyed).
- **Standard Headers**:
  - `event-id`: `outbox_events.id` (UUID string)
  - `event-type`: `outbox_events.event_type`
  - `tenant-code`: `outbox_events.tenant_code`
  - `content-type`: `application/json`
