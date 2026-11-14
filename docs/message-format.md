# Message format (v1)

The neutral message is what consumers receive from `GET /v1/messages` and `GET /v1/events`. The format is versioned by the `/v1` path.

```json
{
  "seq": 1042,
  "receivedAt": "2026-11-14T10:00:00.123Z",
  "timestamp": 1763114400000,
  "kind": "message",
  "sender": {"number": "+4915187654321", "uuid": "0b6a4f2e-1c3d-4e5f-8a9b-0c1d2e3f4a5b", "name": "Owner", "device": 1},
  "group": {"id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=", "internalId": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=", "restId": "group.QUFFQ0F3UUZCZ2NJQ1FvTERBME9EeEFSRWhNVUZSWVhHQmthR3h3ZEhoOD0=", "name": "dsh"},
  "text": "please fix the build",
  "quote": {"timestamp": 1763114300000, "author": {"number": "+4915112345678", "uuid": "9f8e7d6c-5b4a-4321-8fed-cba987654321"}, "text": "Tests failed"},
  "mentions": [{"start": 0, "length": 1, "number": "+4915112345678", "uuid": "9f8e7d6c-5b4a-4321-8fed-cba987654321", "name": "bot"}],
  "attachments": [{"id": "x1y2.jpg", "contentType": "image/jpeg", "size": 48213, "filename": "shot.jpg", "caption": ""}],
  "editOf": 0,
  "reaction": null,
  "pollVote": null,
  "raw": {"envelope": {}, "account": "+4915112345678"}
}
```

## Message

| Field | Type | Description |
| --- | --- | --- |
| `seq` | integer | Position in the hub's log. Order is always by `seq`. |
| `receivedAt` | string | Hub receive time, RFC 3339 with milliseconds, UTC. Ages and cut-offs use this, never `timestamp`. |
| `timestamp` | integer | Signal envelope timestamp in milliseconds. |
| `kind` | string | See [Kinds](#kinds). |
| `sender` | object | See [Sender](#sender). |
| `group` | object or `null` | See [Group](#group). `null` for direct messages. |
| `text` | string | Message body; empty when the kind carries none. |
| `quote` | object or `null` | See [Quote](#quote). |
| `mentions` | array | See [Mention](#mention). `[]` when empty. |
| `attachments` | array | See [Attachment](#attachment). `[]` when empty. |
| `editOf` | integer | Timestamp of the edited message for kind `edit`, else `0`. |
| `reaction` | object or `null` | See [Reaction](#reaction). Set for kind `reaction`. |
| `pollVote` | object or `null` | See [PollVote](#pollvote). Set for kind `poll_vote`. |
| `raw` | object | The original WebSocket frame as JSON (`envelope` and `account`). Whitespace may differ from what signal-cli sent. |

## Kinds

| Kind | Delivered by the generic API |
| --- | --- |
| `message` | yes |
| `edit` | yes |
| `reaction` | yes |
| `poll_vote` | yes |
| `receipt` | compat mode only |
| `typing` | compat mode only |
| `sync` | compat mode only |
| `other` | compat mode only |

Precedence when an envelope carries several parts: `editMessage` > `dataMessage.reaction` > `dataMessage.pollVote` > `dataMessage` > `syncMessage` > `receiptMessage` > `typingMessage` > other. For `edit`, `text`, `quote`, `mentions`, `attachments` and `group` come from `editMessage.dataMessage`.

## Sender

| Field | Type | Description |
| --- | --- | --- |
| `number` | string | E.164 number, empty when unknown. |
| `uuid` | string | Signal account UUID, empty when unknown. |
| `name` | string | Profile name as signal-cli reports it. |
| `device` | integer | Sending device id. |

## Group

| Field | Type | Description |
| --- | --- | --- |
| `id` | string | Same as `internalId`. |
| `internalId` | string | signal-cli's base64 group id (32 bytes). |
| `restId` | string | `group.` plus base64 of `internalId`; the recipient form the REST API takes. |
| `name` | string | Group name; empty for kinds that do not carry it (for example `typing`). |

A group id that is missing or not a valid internal id leaves `group` as `null`, and the hub treats the message as unclaimed instead of routing it by sender.

## Quote

| Field | Type | Description |
| --- | --- | --- |
| `timestamp` | integer | Timestamp of the quoted message. |
| `author` | object | `{"number", "uuid"}` of the quoted author. |
| `text` | string | Quoted text. |

## Mention

| Field | Type | Description |
| --- | --- | --- |
| `start` | integer | Offset of the mention placeholder in `text`. |
| `length` | integer | Length of the placeholder. |
| `number` | string | Mentioned number. |
| `uuid` | string | Mentioned UUID. |
| `name` | string | Display name as signal-cli reports it. |

## Attachment

| Field | Type | Description |
| --- | --- | --- |
| `id` | string | Id for `GET /v1/attachments/{id}`. |
| `contentType` | string | MIME type. |
| `size` | integer | Size in bytes. |
| `filename` | string | Original file name, empty when absent. |
| `caption` | string | Caption, empty when absent. |

## Reaction

| Field | Type | Description |
| --- | --- | --- |
| `emoji` | string | The reaction emoji. |
| `targetAuthor` | object | `{"number", "uuid"}` of the reacted-to message's author. |
| `targetTimestamp` | integer | Timestamp of the reacted-to message. |
| `remove` | boolean | `true` when the reaction was removed. |

## PollVote

| Field | Type | Description |
| --- | --- | --- |
| `targetTimestamp` | integer | Timestamp of the poll message. |
| `optionIndexes` | array of integers | Selected option indexes. |
