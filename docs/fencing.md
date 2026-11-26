# Coordinator fencing tokens (LM-039)

When Coordinator leadership is enabled, `EtcdLeadership` reads the etcd
leadership key's `create_revision` after acquiring the lease. That cluster-wide
revision is the Coordinator term's fencing token: recreating the key for a
later lease yields a larger token. The public redemption request persists the
current token in `redemptions.fencing_token`; all Saga commands and 2PC prepare
requests carry that same value. With leadership disabled for local/test use,
the Coordinator uses token `0`.

Wallet and Catalog each own a `participant_fencing_state` high-water mark in
their own database. Before applying a Saga command or starting a 2PC prepare,
the participant atomically advances the mark if the supplied token is greater
than or equal to the stored value. A lower token is rejected before business
state is changed. The rejection is written to the participant's
`fencing_rejections` table; Saga consumers also return a result with reason
`stale_fencing_token`, while 2PC prepare returns HTTP 409 so the Coordinator
records a NO vote and decides abort.

The participant uses a database advisory lock around fencing and 2PC prepare
so a newer term cannot advance the high-water mark while an older prepare is
still starting. Saga checks execute in the same transaction as their inbox,
business mutation and outbox result.

This is the standard downstream fencing guarantee: an old token is rejected
after the participant has observed a newer token. It does not prove to a
participant that an unseen token is stale before any newer-term command has
reached that participant. A future leadership integration may proactively
advance both participant high-water marks on term acquisition if that stronger
property is required.

2PC COMMIT/ABORT endpoints remain decision-finalization operations for a
transaction already prepared; the fencing check applies to the prepare that
begins participant work. Do not use a fencing rejection to override a durable
2PC decision. Inspect rejections with:

```sql
SELECT transaction_id, supplied_token, highest_token, command_type,
       coordination_mode, created_at
FROM fencing_rejections
ORDER BY id DESC;
```

The schema changes are additive. Apply them through the normal service
migrations before starting updated Wallet and Catalog instances.
