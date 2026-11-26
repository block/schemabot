# Throttling

A throttle holds a copy back while the database is under pressure. Only an
engine that copies has a rate to hold back, and each one does it differently:

| Engine | How it behaves under pressure |
|---|---|
| **MySQL (Spirit)** | Paces. It changes the size of the work, easing off under load and taking more headroom when there is some. SchemaBot reports both the throttle and its reason, so a throttled change reads as throttled. See [throttling in the MySQL guide](mysql.md#throttling) |
| **Vitess (PlanetScale)** | Gates. The copy stays the same size and is let through less often. The thresholds, and the ratio an operator can set on a running deploy request, live on the PlanetScale side, so a throttled change reads in SchemaBot as one going slowly. See [throttling in the Vitess guide](vitess.md#throttling) |
| **PostgreSQL (pg-sprite)** | Neither yet. There is no copy to hold back, so each statement carries a lock timeout and a statement timeout instead, and one that runs past its budget is cancelled and rolled back. See [what pg-sprite does](postgresql.md#what-pg-sprite-does) |

In every case the mechanism belongs to the engine, not to SchemaBot, and it is
the engine's protection rather than a promise about your workload: capacity you
are already using is capacity the change will compete for.

[Load management](engines.md#load-management) compares the three side by side
and explains why they differ.
