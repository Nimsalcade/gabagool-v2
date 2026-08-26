# V7.2 January Portfolio Controller

V7.2 fixes the replenishment defect observed in the first V7.1 live-paper window.

## Core rule

A target parent count is no longer interpreted as a perpetual replenishment target on the overweight side.

- Within 0.5 parent clip of balance: both UP and DOWN may create/replenish resting BUY parents.
- Outside the neutral band: only the underweight side may create fresh/replacement parents.
- Existing overweight-side parents may remain live and partial-fill, subject to the normal economic, staleness and excess-count checks.
- If an overweight-side parent fills or is cancelled while the side remains overweight, it is not replaced.
- If inventory crosses through balance, fresh replenishment permission flips automatically.

This preserves asynchronous fills, queue persistence and controlled overshoot while preventing one nominal overweight parent slot from becoming unlimited cumulative overweight accumulation.

## Economic control

V7.2 deliberately leaves the V7.1 portfolio VWAP cap unchanged for the first A/B test. The replenishment defect is isolated before changing price/economic admission.

## Safety

Read-only paper harness. No wallet is loaded and no real order is sent.
