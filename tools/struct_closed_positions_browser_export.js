/*
Struct Explorer closed-position exporter (browser console, no API key).

Usage:
  1) Open the target trader on https://explorer.struct.to/traders/<wallet>?tab=closed
  2) Set the desired filters/sort in the UI first.
  3) DevTools -> Console -> paste this whole file -> Enter.

The script keeps the current closed-position filters/sort, iterates closedPage
inside a hidden same-origin iframe, extracts the main positions table, and
finally downloads JSON + CSV. It does not sign in, trade, or modify anything.

Stop at any time with:
  window.__STRUCT_EXPORT_CANCEL = true
*/
(async () => {
  'use strict';

  const CONFIG = {
    startPage: 1,
    maxPages: 5000,          // safety ceiling; normal stop is first empty/repeated page
    settleMs: 350,           // wait after iframe load for hydration/streaming
    betweenPagesMs: 75,
    downloadJson: true,
    downloadCsv: true,
  };

  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const clean = (s) => String(s ?? '').replace(/\s+/g, ' ').trim();
  const csvCell = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;

  const base = new URL(location.href);
  if (base.origin !== 'https://explorer.struct.to') {
    throw new Error('Run this on explorer.struct.to');
  }
  if (!/^\/traders\/0x[a-fA-F0-9]{40}$/.test(base.pathname)) {
    throw new Error('Open a trader page first.');
  }

  const wallet = base.pathname.split('/').pop().toLowerCase();
  base.searchParams.set('tab', 'closed');
  base.searchParams.delete('winsPage');
  base.searchParams.delete('lossesPage');

  // Preserve the exact filters and sort selected in the current URL.
  // Only closedPage changes while exporting.
  window.__STRUCT_EXPORT_CANCEL = false;

  const frame = document.createElement('iframe');
  Object.assign(frame.style, {
    position: 'fixed',
    width: '1px',
    height: '1px',
    right: '0',
    bottom: '0',
    opacity: '0.01',
    pointerEvents: 'none',
    zIndex: '-1',
  });
  frame.setAttribute('aria-hidden', 'true');
  document.body.appendChild(frame);

  const loadFrame = (url) => new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timeout loading ${url}`)), 30000);
    frame.onload = () => {
      clearTimeout(timer);
      resolve();
    };
    frame.src = url;
  });

  function findPositionsTable(doc) {
    const tables = [...doc.querySelectorAll('table')];
    const candidates = tables.map(table => {
      const headers = [...table.querySelectorAll('thead th')].map(x => clean(x.textContent));
      const rows = [...table.querySelectorAll('tbody tr')];
      const headerText = headers.join(' | ').toLowerCase();
      let score = 0;
      for (const needle of ['market', 'entry', 'pnl', 'buys', 'sells', 'last active']) {
        if (headerText.includes(needle)) score++;
      }
      return { table, headers, rows, score };
    }).filter(x => x.score >= 4 && x.rows.length > 0);

    if (!candidates.length) return null;
    // Main positions page normally has the largest row count (25); highlight tables are smaller.
    candidates.sort((a, b) => b.rows.length - a.rows.length || b.score - a.score);
    return candidates[0];
  }

  function extractRow(tr, headers, page, rowOnPage) {
    const cells = [...tr.querySelectorAll('td')];
    const out = {
      wallet,
      page,
      row_on_page: rowOnPage,
    };

    cells.forEach((td, i) => {
      const key = clean(headers[i] || `column_${i + 1}`)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_|_$/g, '') || `column_${i + 1}`;
      out[key] = clean(td.textContent);
    });

    const marketLink = tr.querySelector('a[href^="/markets/"], a[href*="polymarket.com/event/"]');
    out.market_href = marketLink ? new URL(marketLink.getAttribute('href'), base.origin).href : '';
    out.market_slug = '';
    if (marketLink) {
      const href = marketLink.getAttribute('href') || '';
      const m = href.match(/\/markets\/([^/?#]+)/) || href.match(/\/event\/([^/?#]+)/);
      if (m) out.market_slug = m[1];
    }

    const badges = [...tr.querySelectorAll('[class*="badge"], [data-slot="badge"]')]
      .map(x => clean(x.textContent)).filter(Boolean);
    out.badges = [...new Set(badges)].join('|');
    out.raw_text = clean(tr.textContent);
    return out;
  }

  const all = [];
  const seenPageFingerprints = new Set();
  let stopReason = 'maxPages';

  try {
    for (let page = CONFIG.startPage; page < CONFIG.startPage + CONFIG.maxPages; page++) {
      if (window.__STRUCT_EXPORT_CANCEL) {
        stopReason = 'cancelled';
        break;
      }

      const u = new URL(base.href);
      u.searchParams.set('closedPage', String(page));

      console.log(`[Struct export] loading closedPage=${page} rows=${all.length}`);
      await loadFrame(u.href);
      await sleep(CONFIG.settleMs);

      const doc = frame.contentDocument;
      if (!doc) throw new Error('Could not access iframe document (same-origin expected).');

      const found = findPositionsTable(doc);
      if (!found) {
        // Empty final page can render as text instead of a table.
        const body = clean(doc.body?.innerText || '');
        if (/no positions to show/i.test(body)) {
          stopReason = 'emptyPage';
          break;
        }
        throw new Error(`Could not identify positions table on closedPage=${page}`);
      }

      const { headers, rows } = found;
      const pageRows = rows.map((tr, i) => extractRow(tr, headers, page, i + 1));
      if (!pageRows.length) {
        stopReason = 'emptyPage';
        break;
      }

      const fp = pageRows.slice(0, 3).map(r => `${r.market_slug}|${r.raw_text}`).join('||');
      if (seenPageFingerprints.has(fp)) {
        stopReason = 'repeatedPage';
        console.warn(`[Struct export] repeated page detected at ${page}; stopping to prevent an infinite loop.`);
        break;
      }
      seenPageFingerprints.add(fp);
      all.push(...pageRows);

      console.log(`[Struct export] page=${page} +${pageRows.length} total=${all.length}`);
      await sleep(CONFIG.betweenPagesMs);
    }
  } finally {
    frame.remove();
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const sortBy = base.searchParams.get('closedSortBy') || '';
  const sortDirection = base.searchParams.get('closedSortDirection') || '';
  const meta = {
    exported_at: new Date().toISOString(),
    source: base.href,
    wallet,
    rows: all.length,
    stop_reason: stopReason,
    closed_sort_by: sortBy,
    closed_sort_direction: sortDirection,
    positions_category: base.searchParams.get('positionsCategory') || '',
    positions_combo: base.searchParams.get('positionsCombo') || '',
  };

  function save(name, text, type) {
    const blob = new Blob([text], { type });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = name;
    document.body.appendChild(a);
    a.click();
    setTimeout(() => {
      URL.revokeObjectURL(a.href);
      a.remove();
    }, 1000);
  }

  if (CONFIG.downloadJson) {
    save(
      `struct-${wallet}-closed-${stamp}.json`,
      JSON.stringify({ meta, rows: all }, null, 2),
      'application/json'
    );
  }

  if (CONFIG.downloadCsv && all.length) {
    const keys = [...new Set(all.flatMap(r => Object.keys(r)))];
    const csv = [
      keys.map(csvCell).join(','),
      ...all.map(r => keys.map(k => csvCell(r[k])).join(',')),
    ].join('\n');
    save(`struct-${wallet}-closed-${stamp}.csv`, csv, 'text/csv');
  }

  console.log('[Struct export] COMPLETE', meta);
  console.table(all.slice(0, 5));
  window.__STRUCT_LAST_EXPORT = { meta, rows: all };
})();
