/*
MetaMask Predictions DOM price recorder (read-only)

Run this directly in DevTools Console on the MetaMask Predictions market page.
It never clicks, signs, trades, or touches wallet state. It only observes DOM text.

Controls after start:
  __MM_DOM_RECORDER__.status()
  __MM_DOM_RECORDER__.downloadCSV()
  __MM_DOM_RECORDER__.downloadJSON()
  __MM_DOM_RECORDER__.stop()

The recorder survives React button replacement/remounts because it observes the
document body rather than attaching observers to the current Up/Down buttons.
*/
(() => {
  'use strict';

  const GLOBAL = '__MM_DOM_RECORDER__';
  const old = window[GLOBAL];
  if (old && typeof old.stop === 'function') {
    try { old.stop(); } catch (_) {}
  }

  const startedEpochMs = Date.now();
  const startedPerfMs = performance.now();
  const rows = [];
  let stopped = false;
  let scheduled = false;
  let lastKey = null;
  let mutationCount = 0;
  let sampleCount = 0;

  function nowIso(ms = Date.now()) {
    return new Date(ms).toISOString();
  }

  function normalizeText(v) {
    return (v ?? '').replace(/\u00a0/g, ' ').trim();
  }

  function parsePrice(rawText) {
    const raw = normalizeText(rawText);
    if (!raw || raw === '--' || raw === '—' || raw === '-') {
      return { raw, valid: false, cents: null, dollars: null };
    }

    // MetaMask commonly renders prediction prices as e.g. 0.1¢, 25¢, 99.9¢.
    const centMatch = raw.match(/^([0-9]+(?:\.[0-9]+)?)\s*¢$/);
    if (centMatch) {
      const cents = Number(centMatch[1]);
      return {
        raw,
        valid: Number.isFinite(cents),
        cents,
        dollars: Number.isFinite(cents) ? cents / 100 : null,
      };
    }

    // Defensive support if the UI switches to dollar formatting.
    const dollarMatch = raw.match(/^\$\s*([0-9]+(?:\.[0-9]+)?)$/);
    if (dollarMatch) {
      const dollars = Number(dollarMatch[1]);
      return {
        raw,
        valid: Number.isFinite(dollars),
        cents: Number.isFinite(dollars) ? dollars * 100 : null,
        dollars: Number.isFinite(dollars) ? dollars : null,
      };
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

    // Fallbacks based on the DOM supplied from MetaMask Predictions.
    const color = normalizeText(button.getAttribute('data-color')).toLowerCase();
    if (color === 'green') return 'UP';
    if (color === 'red') return 'DOWN';

    const all = normalizeText(button.textContent).toLowerCase();
    if (/\bup\b/.test(all)) return 'UP';
    if (/\bdown\b/.test(all)) return 'DOWN';
    return null;
  }

  function priceTextForButton(button) {
    // In the supplied DOM the second span is the price:
    // <span class="text-base font-semibold">0.1¢</span>
    const preferred = button.querySelector('.trading-button-text .text-base.font-semibold');
    if (preferred) return normalizeText(preferred.textContent);

    // Fallback: choose a descendant containing a recognizable price token.
    const descendants = Array.from(button.querySelectorAll('span'));
    for (const el of descendants) {
      const t = normalizeText(el.textContent);
      if (t === '--' || /¢$/.test(t) || /^\$\s*[0-9]/.test(t)) return t;
    }
    return '';
  }

  function readSide(side) {
    const buttons = findTradingButtons();
    let chosen = null;
    for (const button of buttons) {
      if (labelForButton(button) === side) {
        chosen = button;
        break;
      }
    }

    if (!chosen) {
      return {
        found: false,
        selected: null,
        disabled: null,
        raw: '',
        valid: false,
        cents: null,
        dollars: null,
      };
    }

    const parsed = parsePrice(priceTextForButton(chosen));
    return {
      found: true,
      selected: chosen.getAttribute('data-selected'),
      disabled: Boolean(chosen.disabled),
      ...parsed,
    };
  }

  function currentMarketHint() {
    const url = location.href;
    const title = normalizeText(document.title);
    const h1 = normalizeText(document.querySelector('h1')?.textContent);
    return { url, title, h1 };
  }

  function snapshot(reason = 'mutation', force = false) {
    if (stopped) return null;

    const epochMs = Date.now();
    const perfMs = performance.now();
    const up = readSide('UP');
    const down = readSide('DOWN');

    const pair = up.valid && down.valid ? up.dollars + down.dollars : null;
    const edge = pair == null ? null : 1 - pair;
    const key = JSON.stringify([
      up.raw, down.raw,
      up.found, down.found,
      up.selected, down.selected,
      up.disabled, down.disabled,
      location.href,
    ]);

    if (!force && key === lastKey) return null;
    lastKey = key;
    sampleCount += 1;

    const market = currentMarketHint();
    const row = {
      seq: rows.length + 1,
      epoch_ms: epochMs,
      iso_utc: nowIso(epochMs),
      elapsed_ms: perfMs - startedPerfMs,
      reason,
      url: market.url,
      document_title: market.title,
      heading: market.h1,
      up_raw: up.raw,
      up_cents: up.cents,
      up_dollars: up.dollars,
      up_found: up.found,
      up_selected: up.selected,
      down_raw: down.raw,
      down_cents: down.cents,
      down_dollars: down.dollars,
      down_found: down.found,
      down_selected: down.selected,
      pair_dollars: pair,
      pair_under_1: pair == null ? null : pair < 1,
      edge_per_pair: edge,
      edge_5_shares: edge == null ? null : edge * 5,
    };

    rows.push(row);

    const pairText = pair == null ? '--' : pair.toFixed(4);
    const edge5Text = edge == null ? '--' : (edge * 5).toFixed(4);
    console.log(
      `[MM-DOM #${row.seq}] ${row.iso_utc} ` +
      `UP=${up.raw || '(missing)'} DOWN=${down.raw || '(missing)'} ` +
      `pair=$${pairText} edge5=$${edge5Text}`
    );
    return row;
  }

  function schedule(reason) {
    if (stopped || scheduled) return;
    scheduled = true;
    queueMicrotask(() => {
      scheduled = false;
      snapshot(reason, false);
    });
  }

  const observer = new MutationObserver((mutations) => {
    mutationCount += mutations.length;
    schedule('mutation');
  });

  const target = document.body || document.documentElement;
  observer.observe(target, {
    subtree: true,
    childList: true,
    characterData: true,
    attributes: true,
    attributeFilter: ['data-selected', 'data-color', 'disabled'],
  });

  // Safety net for frameworks that update via mechanisms which happen not to
  // generate a useful mutation at the exact price node. Dedup prevents spam.
  const interval = setInterval(() => snapshot('safety_poll', false), 250);

  function csvEscape(value) {
    if (value == null) return '';
    const s = String(value);
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  }

  function csvText() {
    const fields = [
      'seq','epoch_ms','iso_utc','elapsed_ms','reason','url','document_title','heading',
      'up_raw','up_cents','up_dollars','up_found','up_selected',
      'down_raw','down_cents','down_dollars','down_found','down_selected',
      'pair_dollars','pair_under_1','edge_per_pair','edge_5_shares'
    ];
    const lines = [fields.join(',')];
    for (const row of rows) {
      lines.push(fields.map((f) => csvEscape(row[f])).join(','));
    }
    return lines.join('\n');
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

  function stamp() {
    return new Date(startedEpochMs).toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
  }

  const api = {
    rows,
    startedEpochMs,
    snapshot: () => snapshot('manual', true),
    status() {
      const last = rows.length ? rows[rows.length - 1] : null;
      const status = {
        running: !stopped,
        rows: rows.length,
        mutations_seen: mutationCount,
        samples_recorded: sampleCount,
        started_utc: nowIso(startedEpochMs),
        last,
      };
      console.table(status);
      return status;
    },
    downloadCSV() {
      downloadBlob(csvText(), 'text/csv;charset=utf-8', `metamask_dom_prices_${stamp()}.csv`);
    },
    downloadJSON() {
      downloadBlob(JSON.stringify({
        started_utc: nowIso(startedEpochMs),
        stopped_utc: stopped ? nowIso() : null,
        page_url: location.href,
        rows,
      }, null, 2), 'application/json;charset=utf-8', `metamask_dom_prices_${stamp()}.json`);
    },
    stop() {
      if (stopped) return;
      stopped = true;
      observer.disconnect();
      clearInterval(interval);
      console.log(`[MM-DOM] stopped; captured ${rows.length} price-state changes.`);
    },
  };

  window[GLOBAL] = api;
  snapshot('start', true);
  console.log(
    '[MM-DOM] recorder running. Read-only. Controls: ' +
    '__MM_DOM_RECORDER__.status(), .downloadCSV(), .downloadJSON(), .stop()'
  );
})();
