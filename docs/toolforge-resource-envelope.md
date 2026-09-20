# Permanent Toolforge resource envelope

**Status:** binding operating contract  
**Adopted:** 2026-09-20  
**Policy:** `toolforge-current-envelope-no-expansion-v1`

Wiki Economics will not assume, request, or design around additional
Toolforge CPU or memory for enwiki. The resources currently granted are the
permanent planning envelope for this project:

| Resource | Hard ceiling | Operational rule |
| --- | ---: | --- |
| Namespace CPU | 16 vCPU | Keep heavy stages sequential; aggregate quota is not permission to run multiple full refreshes. |
| Namespace memory | 24 GiB | Includes the resident service and every active job. |
| Memory per job | 6 GiB | Every enwiki stage must fit here; a 16 GiB job is not an available fallback. |
| CPU per job | 4 vCPU | The current wrappers remain conservatively single-threaded until a measured profile proves otherwise. |
| Resident service reservation | 0.5 GiB / 1 vCPU | Must remain available while a batch stage runs. |
| Shared NFS | No private quota | Admission must retain the configured reserve and must not treat shared free space as a guarantee. |

The memory qualification gate is therefore **at most 4.5 GiB peak in a 6 GiB
job**, preserving the existing 25% headroom requirement. More CPU cannot make an
over-memory algorithm safe.

Enwiki may be admitted only through bounded, sequential stages—ingest/source
cleanup, monthly metrics, lifecycle, page-week, patrol/validation, and
merge/publication—without concurrent heavy enwiki jobs. Source windows must be
small, every validated compressed source must be released, and the 250 GiB
storage preflight reserve remains mandatory. A larger temporary quota, a 16 GiB
job, or a future Toolforge grant is not part of the plan.

If the bounded implementation cannot pass these limits, the result is
**not feasible under the current contract** and the algorithm must be reduced,
partitioned, or otherwise redesigned. The project must not solve that failure
by silently assuming more capacity.

Changing this envelope is outside normal onboarding. It requires an explicit
operator decision, an updated capacity receipt, refreshed admission policy, and
new deterministic/recovery qualification. Until then, all documentation,
capacity experiments, and production gates must use these ceilings.
