/*
MetaMask Predictions DOM shadow lab (read-only)

Purpose
-------
Observe the current UP/DOWN prices shown in the DOM and, at the same time,
run several independent SHADOW maker models.  It never clicks, signs, trades,
or touches wallet state.

This is a hypothesis tester, not a real fill oracle.  The DOM prices are the
current displayed buy prices.  A shadow resting bid at P is treated as filled
only after a later observed displayed price reaches (TOUCH) or passes through
(PASS) that quote.  That avoids counting every unchanged below-$1 snapshot as
another acquisition, but it still does not model queue position or depth.

15m historical test preset
---------------------------
The clip ladder is deliberately based on the Oct-29 first-market fingerprint
and is kept as a TEST PRESET rather than asserted as the true bot rule:
  age 180-195s: 10 shares
      195-390s:  9
      390-540s:  8
      540-665s:  7
      665-690s:  6

Controls after start
--------------------
  __MM_SHADOW_LAB__.status()
  __MM_SHADOW_LAB__.downloadJSON()
  __MM_SHADOW_LAB__.downloadFillsCSV()
  __MM_SHADOW_LAB__.downloadPricesCSV()
  __MM_SHADOW_LAB__.togglePanel()
  __MM_SHADOW_LAB__.stop()
*/
(() => {
  'use strict';

  const GLOBAL = '__MM_SHADOW_LAB__';
  const old = window[GLOBAL];
  if (old && typeof old.stop === 'function') {
    try { old.stop(); } catch (_) {}
  }

  const CONFIG = {
    pollMs: 250,
    requoteMs: 12000,
    rearmMs: 750,
    activeStartSec15m: 180,
    activeEndSec15m: 690,
    maxGapShares: 60,
    skewThresholdShares: 10,
    engines: [
      {
        name: 'SYM_TOUCH_1C',
        baseOffsetCents: 1,
        fillThroughCents: 0,
        inventorySkew: false,
        maxPairBasis: null,
      },
      {
        name: 'SYM_PASS_1C',
        baseOffsetCents: 1,
        fillThroughCents: 1,
        inventorySkew: false,
        maxPairBasis: null,
      },
      {
        name: 'SKEW_TOUCH_2C',
        baseOffsetCents: 2,
        fillThroughCents: 0,
        inventorySkew: true,
        maxPairBasis: null,
      },
      {
        name: 'SKEW_PASS_2C',
        baseOffsetCents: 2,
        fillThroughCents: 1,
        inventorySkew: true,
        maxPairBasis: null,
      },
      {
        name: 'SKEW_PASS_CAP100',
        baseOffsetCents: 2,
        fillThroughCents: 1,
        inventorySkew: true,
        maxPairBasis: 1.0,
      },
    ],
  };

  const startedEpochMs = Date.now();
  const startedPerfMs = performance.now();
  const priceRows = [];
  const actionRows = [];
  let stopped = false;
  let lastPriceKey = null;
  let mutationCount = 0;
  let panelVisible = true;

  function nowIso(ms = Date.now()) { return new Date(ms).toISOString(); }
  function normalizeText(v) { return (v ?? '').replace(/\u00a0/g, ' ').trim(); }
  function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
  function r6(v) { return v == null ? null : Number(v.toFixed(6)); }

  function parsePrice(rawText) {
    const raw = normalizeText(rawText);
    if (!raw || raw === '--' || raw === '—' || raw === '-') {
      return { raw, valid: false, cents: null, dollars: null };
    }
    let m = raw.match(/^([0-9]+(?:\.[0-9]+)?)\s*¢$/);
    if (m) {
      const cents = Number(m[1]);
      return { raw, valid: Number.isFinite(cents), cents, dollars: cents / 100 };
    }
    m = raw.match(/^\$\s*([0-9]+(?:\.[0-9]+)?)$/);
    if (m) {
      const dollars = Number(m[1]);
      return { raw, valid: Number.isFinite(dollars), cents: dollars * 100, dollars };
    }
    return { raw, valid: false, cents: null, dollars: null };
  }

  function findTradingButtons() {
    return Array.from(document.querySelectorAll('button.trading-button'));
  }

  function labelForButton(button) {
    const explicit = button.querySelector('.trading-button-text .text-sm');
    const txt = normalizeText(explicit?.textContent).toLowerCase();
    if (txt === 'up' || txt === 'down') return txt.toUpperCase();
    const color = normalizeText(button.getAttribute('data-color')).toLowerCase();
    if (color === 'green') return 'UP';
    if (color === 'red') return 'DOWN';
    const all = normalizeText(button.textContent).toLowerCase();
    if (/\bup\b/.test(all)) return 'UP';
    if (/\bdown\b/.test(all)) return 'DOWN';
    return null;
  }

  function priceTextForButton(button) {
    const preferred = button.querySelector('.trading-button-text .text-base.font-semibold');
    if (preferred) return normalizeText(preferred.textContent);
    for (const el of Array.from(button.querySelectorAll('span'))) {
      const t = normalizeText(el.textContent);
      if (t === '--' || /¢$/.test(t) || /^\$\s*[0-9]/.test(t)) return t;
    }
    return '';
  }

  function readSide(side) {
    const button = findTradingButtons().find((b) => labelForButton(b) === side);
    if (!button) return { found: false, raw: '', valid: false, cents: null, dollars: null };
    return { found: true, ...parsePrice(priceTextForButton(button)) };
  }

  function parseMarket() {
    const href = location.href;
    const m = href.match(/(?:btc|eth)-updown-(5m|15m)-(\d{10})/i);
    const asset = /btc-updown/i.test(href) ? 'BTC' : (/eth-updown/i.test(href) ? 'ETH' : 'UNKNOWN');
    if (!m) {
      return {
        asset,
        durationSec: null,
        startEpochSec: null,
        ageSec: (Date.now() - startedEpochMs) / 1000,
        slug: null,
      };
    }
    const durationSec = m[1].toLowerCase() === '15m' ? 900 : 300;
    const startEpochSec = Number(m[2]);
    return {
      asset,
      durationSec,
      startEpochSec,
      ageSec: Date.now() / 1000 - startEpochSec,
      slug: href.match(/(?:btc|eth)-updown-(?:5m|15m)-\d{10}/i)?.[0] || null,
    };
  }

  function historicalClipSize(market) {
    const a = market.ageSec;
    if (market.durationSec === 900) {
      if (a < CONFIG.activeStartSec15m || a > CONFIG.activeEndSec15m) return 0;
      if (a < 195) return 10;
      if (a < 390) return 9;
      if (a < 540) return 8;
      if (a < 665) return 7;
      return 6;
    }
    // We do not have a proven 5m clip schedule from the Oct-29 15m fingerprint.
    // Use a flat 9-share CONTROL rather than pretending the 15m ladder is known.
    if (market.durationSec === 300) return market.ageSec >= 0 && market.ageSec <= 285 ? 9 : 0;
    return 9;
  }

  function newEngine(spec) {
    return {
      spec: { ...spec },
      quotes: { UP: null, DOWN: null },
      nextArmMs: { UP: 0, DOWN: 0 },
      shares: { UP: 0, DOWN: 0 },
      cost: { UP: 0, DOWN: 0 },
      fills: { UP: 0, DOWN: 0 },
      maxAbsGap: 0,
      signFlips: 0,
      lastNonZeroGapSign: 0,
      blockedBasis: 0,
      blockedGap: 0,
      firstFillMs: null,
      lastFillMs: null,
    };
  }

  const engines = CONFIG.engines.map(newEngine);

  function engineSummary(e) {
    const upShares = e.shares.UP;
    const downShares = e.shares.DOWN;
    const upAvg = upShares > 0 ? e.cost.UP / upShares : null;
    const downAvg = downShares > 0 ? e.cost.DOWN / downShares : null;
    const pairBasis = upAvg != null && downAvg != null ? upAvg + downAvg : null;
    const matched = Math.min(upShares, downShares);
    const grossMatchedEdge = pairBasis == null ? null : matched * (1 - pairBasis);
    const gap = upShares - downShares;
    return {
      engine: e.spec.name,
      up_fills: e.fills.UP,
      down_fills: e.fills.DOWN,
      up_shares: r6(upShares),
      down_shares: r6(downShares),
      up_vwap: r6(upAvg),
      down_vwap: r6(downAvg),
      pair_basis: r6(pairBasis),
      matched_shares: r6(matched),
      gross_matched_edge: r6(grossMatchedEdge),
      gap_up_minus_down: r6(gap),
      max_abs_gap: r6(e.maxAbsGap),
      gap_sign_flips: e.signFlips,
      blocked_basis: e.blockedBasis,
      blocked_gap: e.blockedGap,
      first_fill_iso: e.firstFillMs ? nowIso(e.firstFillMs) : null,
      last_fill_iso: e.lastFillMs ? nowIso(e.lastFillMs) : null,
    };
  }

  function projectedPairBasis(e, side, qty, priceCents) {
    let us = e.shares.UP, ds = e.shares.DOWN;
    let uc = e.cost.UP, dc = e.cost.DOWN;
    const px = priceCents / 100;
    if (side === 'UP') { us += qty; uc += qty * px; }
    else { ds += qty; dc += qty * px; }
    if (us <= 0 || ds <= 0) return null;
    return uc / us + dc / ds;
  }

  function logAction(e, action, side, detail = {}) {
    const market = parseMarket();
    const row = {
      seq: actionRows.length + 1,
      epoch_ms: Date.now(),
      iso_utc: nowIso(),
      market_age_sec: r6(market.ageSec),
      asset: market.asset,
      market_slug: market.slug,
      engine: e.spec.name,
      action,
      side,
      ...detail,
      ...engineSummary(e),
    };
    actionRows.push(row);
    return row;
  }

  function currentOffsetCents(e, side) {
    let off = e.spec.baseOffsetCents;
    if (!e.spec.inventorySkew) return off;
    const gap = e.shares.UP - e.shares.DOWN;
    if (Math.abs(gap) < CONFIG.skewThresholdShares) return off;
    const underweight = gap > 0 ? 'DOWN' : 'UP';
    if (side === underweight) off = Math.max(1, off - 1);
    else off = off + 1;
    return off;
  }

  function wouldBreakGap(e, side, qty) {
    if (!e.spec.inventorySkew) return false;
    const before = e.shares.UP - e.shares.DOWN;
    const after = before + (side === 'UP' ? qty : -qty);
    return Math.abs(after) > CONFIG.maxGapShares && Math.abs(after) > Math.abs(before);
  }

  function candidateQuoteCents(e, side, askCents) {
    const off = currentOffsetCents(e, side);
    // Historical fills were predominantly round cents.  Floor to an integer cent
    // and keep a minimum 1c distance from the displayed buy price.
    return clamp(Math.floor(askCents - off + 1e-9), 1, 99);
  }

  function armQuote(e, side, askCents, qty, reason) {
    if (!Number.isFinite(askCents) || qty <= 0) return;
    if (wouldBreakGap(e, side, qty)) {
      e.blockedGap += 1;
      return;
    }
    const quoteCents = candidateQuoteCents(e, side, askCents);
    const projected = projectedPairBasis(e, side, qty, quoteCents);
    if (e.spec.maxPairBasis != null && projected != null && projected > e.spec.maxPairBasis) {
      e.blockedBasis += 1;
      return;
    }
    e.quotes[side] = {
      side,
      priceCents: quoteCents,
      qty,
      placedMs: Date.now(),
      placedAskCents: askCents,
    };
    logAction(e, reason, side, {
      quote_cents: quoteCents,
      qty,
      observed_ask_cents: askCents,
      projected_pair_basis: r6(projected),
    });
  }

  function fillQuote(e, side, askCents) {
    const q = e.quotes[side];
    if (!q) return false;
    const required = q.priceCents - e.spec.fillThroughCents;
    if (askCents > required + 1e-9) return false;

    const px = q.priceCents / 100;
    e.shares[side] += q.qty;
    e.cost[side] += q.qty * px;
    e.fills[side] += 1;
    const now = Date.now();
    e.firstFillMs ??= now;
    e.lastFillMs = now;

    const gap = e.shares.UP - e.shares.DOWN;
    e.maxAbsGap = Math.max(e.maxAbsGap, Math.abs(gap));
    const sign = gap > 0 ? 1 : gap < 0 ? -1 : 0;
    if (sign !== 0) {
      if (e.lastNonZeroGapSign !== 0 && sign !== e.lastNonZeroGapSign) e.signFlips += 1;
      e.lastNonZeroGapSign = sign;
    }

    logAction(e, 'FILL_ASSUMED', side, {
      quote_cents: q.priceCents,
      qty: q.qty,
      observed_ask_cents: askCents,
      fill_rule: e.spec.fillThroughCents === 0 ? 'TOUCH' : `PASS_${e.spec.fillThroughCents}C`,
      quote_age_ms: now - q.placedMs,
    });

    e.quotes[side] = null;
    e.nextArmMs[side] = now + CONFIG.rearmMs;
    return true;
  }

  function tickEngine(e, up, down, market) {
    const qty = historicalClipSize(market);
    if (qty <= 0 || !up.valid || !down.valid) return;
    const now = Date.now();

    for (const side of ['UP', 'DOWN']) {
      const askCents = side === 'UP' ? up.cents : down.cents;
      if (fillQuote(e, side, askCents)) continue;

      const q = e.quotes[side];
      if (q && now - q.placedMs >= CONFIG.requoteMs) {
        logAction(e, 'CANCEL_STALE', side, {
          quote_cents: q.priceCents,
          qty: q.qty,
          observed_ask_cents: askCents,
          quote_age_ms: now - q.placedMs,
        });
        e.quotes[side] = null;
      }

      if (!e.quotes[side] && now >= e.nextArmMs[side]) {
        armQuote(e, side, askCents, qty, 'QUOTE');
      }
    }
  }

  function capturePrice(reason = 'poll', force = false) {
    if (stopped) return null;
    const up = readSide('UP');
    const down = readSide('DOWN');
    const market = parseMarket();
    const pair = up.valid && down.valid ? up.dollars + down.dollars : null;
    const key = JSON.stringify([up.raw, down.raw, location.href]);
    if (!force && key === lastPriceKey) return { up, down, market, changed: false };
    lastPriceKey = key;

    const row = {
      seq: priceRows.length + 1,
      epoch_ms: Date.now(),
      iso_utc: nowIso(),
      elapsed_ms: performance.now() - startedPerfMs,
      reason,
      asset: market.asset,
      market_slug: market.slug,
      market_age_sec: r6(market.ageSec),
      url: location.href,
      up_raw: up.raw,
      up_cents: up.cents,
      up_dollars: up.dollars,
      down_raw: down.raw,
      down_cents: down.cents,
      down_dollars: down.dollars,
      pair_dollars: pair,
      pair_under_1: pair == null ? null : pair < 1,
      displayed_edge_per_pair: pair == null ? null : 1 - pair,
    };
    priceRows.push(row);
    return { up, down, market, changed: true, row };
  }

  function historicalReference(asset) {
    if (asset === 'BTC') return {
      fills_up: 46, fills_down: 40,
      shares_up: 307.148792, shares_down: 291.813648,
      pair_basis: 0.981796, final_gap: 15.335144,
      max_abs_gap_approx: 56.7,
    };
    if (asset === 'ETH') return {
      fills_up: 40, fills_down: 50,
      shares_up: 319.622856, shares_down: 300.868884,
      pair_basis: 0.982082, final_gap: 18.753972,
      max_abs_gap_approx: 53.7,
    };
    return null;
  }

  function csvEscape(v) {
    if (v == null) return '';
    const s = String(v);
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  }

  function rowsToCsv(rows) {
    if (!rows.length) return '';
    const fields = Array.from(rows.reduce((set, r) => {
      Object.keys(r).forEach((k) => set.add(k));
      return set;
    }, new Set()));
    return [fields.join(','), ...rows.map((r) => fields.map((f) => csvEscape(r[f])).join(','))].join('\n');
  }

  function stamp() {
    return new Date(startedEpochMs).toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
  }

  function downloadBlob(text, mime, filename) {
    const blob = new Blob([text], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function makePanel() {
    const existing = document.getElementById('__mm_shadow_lab_panel__');
    if (existing) existing.remove();
    const el = document.createElement('div');
    el.id = '__mm_shadow_lab_panel__';
    Object.assign(el.style, {
      position: 'fixed', right: '12px', bottom: '12px', zIndex: '2147483647',
      width: '440px', maxHeight: '48vh', overflow: 'auto',
      background: 'rgba(10,10,14,.94)', color: '#f5f5f5', border: '1px solid #555',
      borderRadius: '8px', padding: '10px', font: '12px/1.35 ui-monospace, SFMono-Regular, Menlo, monospace',
      boxShadow: '0 8px 30px rgba(0,0,0,.35)',
    });
    document.body.appendChild(el);
    return el;
  }

  const panel = makePanel();

  function renderPanel(up, down, market) {
    if (!panelVisible) return;
    const pair = up.valid && down.valid ? (up.dollars + down.dollars) : null;
    const summaries = engines.map(engineSummary);
    const lines = summaries.map((s) => {
      const basis = s.pair_basis == null ? '--' : s.pair_basis.toFixed(4);
      const edge = s.gross_matched_edge == null ? '--' : s.gross_matched_edge.toFixed(2);
      return `${s.engine.padEnd(18)} M=${String(s.matched_shares).padStart(7)} B=${basis} E=$${edge.padStart(6)} G=${String(s.gap_up_minus_down).padStart(7)}`;
    });
    const ref = historicalReference(market.asset);
    panel.innerHTML = `
      <div style="font-weight:700;margin-bottom:5px">MM SHADOW LAB — READ ONLY</div>
      <div>${market.asset} ${market.slug || ''} age=${market.ageSec.toFixed(1)}s</div>
      <div>DOM: UP=${up.raw || '--'} DOWN=${down.raw || '--'} pair=${pair == null ? '--' : pair.toFixed(4)}</div>
      <div style="margin:6px 0;border-top:1px solid #444"></div>
      <pre style="white-space:pre-wrap;margin:0">${lines.join('\n')}</pre>
      ${ref ? `<div style="margin-top:6px;color:#bbb">Oct29 ref: basis=${ref.pair_basis.toFixed(4)} finalGap=${ref.final_gap.toFixed(1)} maxGap≈${ref.max_abs_gap_approx}</div>` : ''}
      <div style="margin-top:5px;color:#aaa">FILL_ASSUMED = touch/pass-through proxy; no queue/depth.</div>
    `;
  }

  const observer = new MutationObserver((mutations) => {
    mutationCount += mutations.length;
    capturePrice('mutation', false);
  });
  observer.observe(document.body || document.documentElement, {
    subtree: true, childList: true, characterData: true, attributes: true,
    attributeFilter: ['data-selected', 'data-color', 'disabled'],
  });

  const interval = setInterval(() => {
    if (stopped) return;
    const snap = capturePrice('poll', false) || {
      up: readSide('UP'), down: readSide('DOWN'), market: parseMarket(), changed: false,
    };
    for (const e of engines) tickEngine(e, snap.up, snap.down, snap.market);
    renderPanel(snap.up, snap.down, snap.market);
  }, CONFIG.pollMs);

  const api = {
    config: CONFIG,
    priceRows,
    actionRows,
    engines,
    status() {
      const market = parseMarket();
      const out = {
        running: !stopped,
        started_utc: nowIso(startedEpochMs),
        mutations_seen: mutationCount,
        price_state_changes: priceRows.length,
        actions: actionRows.length,
        market,
        historical_reference: historicalReference(market.asset),
        engines: engines.map(engineSummary),
      };
      console.table(out.engines);
      console.log(out);
      return out;
    },
    downloadJSON() {
      const market = parseMarket();
      downloadBlob(JSON.stringify({
        schema: 'mm_shadow_lab_v1',
        started_utc: nowIso(startedEpochMs),
        stopped_utc: stopped ? nowIso() : null,
        config: CONFIG,
        market,
        historical_reference: historicalReference(market.asset),
        summaries: engines.map(engineSummary),
        price_rows: priceRows,
        action_rows: actionRows,
      }, null, 2), 'application/json;charset=utf-8', `mm_shadow_lab_${market.asset}_${stamp()}.json`);
    },
    downloadFillsCSV() {
      const fills = actionRows.filter((r) => r.action === 'FILL_ASSUMED');
      downloadBlob(rowsToCsv(fills), 'text/csv;charset=utf-8', `mm_shadow_fills_${parseMarket().asset}_${stamp()}.csv`);
    },
    downloadPricesCSV() {
      downloadBlob(rowsToCsv(priceRows), 'text/csv;charset=utf-8', `mm_shadow_prices_${parseMarket().asset}_${stamp()}.csv`);
    },
    togglePanel() {
      panelVisible = !panelVisible;
      panel.style.display = panelVisible ? 'block' : 'none';
      return panelVisible;
    },
    stop() {
      if (stopped) return;
      stopped = true;
      clearInterval(interval);
      observer.disconnect();
      panel.remove();
      console.log('[MM-SHADOW] stopped.');
      console.table(engines.map(engineSummary));
    },
  };

  window[GLOBAL] = api;
  capturePrice('start', true);
  console.log('[MM-SHADOW] running read-only shadow simulation.');
  console.log('Controls: __MM_SHADOW_LAB__.status(), .downloadJSON(), .downloadFillsCSV(), .downloadPricesCSV(), .togglePanel(), .stop()');
})();
