(async () => {
  const API = "https://api.datadash.xyz/event_analytics.v1.MetadataService/ListTable";
  const WALLET = "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d";
  const AGG_PAGE = 50;
  const RAW_PAGE = 100;
  const RAW_ID_CHUNK = 20;
  const META_ID_CHUNK = 100;

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const chunks = (xs, n) => Array.from({ length: Math.ceil(xs.length / n) }, (_, i) => xs.slice(i * n, (i + 1) * n));
  const datePart = s => String(s || "").slice(0, 10);

  async function post(body, attempt = 1) {
    const r = await fetch(API, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "connect-protocol-version": "1"
      },
      body: JSON.stringify(body)
    });
    if (!r.ok) {
      const txt = await r.text();
      if (attempt < 4 && (r.status === 429 || r.status >= 500)) {
        const wait = 700 * (2 ** (attempt - 1));
        console.warn(`HTTP ${r.status}; retry ${attempt}/3 in ${wait}ms`);
        await sleep(wait);
        return post(body, attempt + 1);
      }
      throw new Error(`HTTP ${r.status}: ${txt.slice(0, 500)}`);
    }
    return r.json();
  }

  function propertyRows(j) {
    return (j?.propertyItems || []).map(x => x?.propertyIdValue || {}).filter(Boolean);
  }

  function walletFilter() {
    return {
      leaf: {
        propertyId: 3,
        categoricalCondition: {
          value: [WALLET],
          operator: "CATEGORICAL_OPERATOR_IN"
        }
      }
    };
  }

  async function discoverFirstDayAggregates() {
    let offset = 0;
    let firstDay = null;
    const selected = [];

    while (true) {
      const body = {
        page: { offset, limit: AGG_PAGE },
        tableId: 1,
        orderBy: [{ propertyId: 2 }],
        activityOptions: {
          activityIdFilter: [1],
          aggregationWindowId: 1
        },
        globalFilter: walletFilter(),
        searchKey: "",
        responsePropertyIds: [3, 1, 11, 5, 6, 8, 7, 9, 10, 2, 16]
      };

      const batch = propertyRows(await post(body));
      if (!batch.length) break;

      if (!firstDay) {
        const firstTs = batch.find(r => datePart(r["2"]))?.["2"];
        if (!firstTs) throw new Error("Could not determine first trading day from aggregated activity.");
        firstDay = datePart(firstTs);
        console.log(`FIRST DAY   ${firstDay} (DataDash timestamp date)`);
      }

      const sameDay = batch.filter(r => datePart(r["2"]) === firstDay);
      selected.push(...sameDay);
      console.log(`AGG         offset=${offset} batch=${batch.length} first-day rows=${selected.length}`);

      const crossedDay = batch.some(r => datePart(r["2"]) && datePart(r["2"]) > firstDay);
      if (crossedDay || batch.length < AGG_PAGE) break;

      offset += AGG_PAGE;
      if (offset > 5000) throw new Error("Aggregate pagination guard hit.");
      await sleep(80);
    }

    return { firstDay, rows: selected };
  }

  async function fetchMetadata(ids) {
    const out = [];
    for (const [i, part] of chunks(ids, META_ID_CHUNK).entries()) {
      const body = {
        page: { limit: META_ID_CHUNK },
        tableId: 6,
        globalFilter: {
          leaf: {
            propertyId: 1,
            categoricalCondition: {
              value: part,
              operator: "CATEGORICAL_OPERATOR_IN"
            }
          }
        }
      };
      const batch = propertyRows(await post(body));
      out.push(...batch);
      console.log(`META        chunk=${i + 1}/${Math.ceil(ids.length / META_ID_CHUNK)} rows=${batch.length}`);
      await sleep(80);
    }
    return out;
  }

  async function fetchRawFills(ids) {
    const all = [];
    const idChunks = chunks(ids, RAW_ID_CHUNK);

    for (let ci = 0; ci < idChunks.length; ci++) {
      const part = idChunks[ci];
      let offset = 0;

      while (true) {
        const body = {
          page: { offset, limit: RAW_PAGE },
          tableId: 1,
          orderBy: [{ propertyId: 2 }],
          activityOptions: {
            activityIdFilter: [1]
          },
          globalFilter: walletFilter(),
          localFilters: [
            {
              leaf: {
                propertyId: 11,
                categoricalCondition: {
                  value: part,
                  operator: "CATEGORICAL_OPERATOR_IN"
                }
              }
            }
          ],
          responsePropertyIds: [3, 1, 11, 5, 6, 7, 9, 2, 16]
        };

        const batch = propertyRows(await post(body));
        all.push(...batch);
        console.log(`RAW         ids=${ci + 1}/${idChunks.length} offset=${offset} batch=${batch.length} total=${all.length}`);
        if (batch.length < RAW_PAGE) break;
        offset += RAW_PAGE;
        if (offset > 10000) throw new Error(`Raw pagination guard hit for ID chunk ${ci + 1}.`);
        await sleep(80);
      }
      await sleep(120);
    }

    all.sort((a, b) => {
      const t = String(a["2"] || "").localeCompare(String(b["2"] || ""));
      if (t !== 0) return t;
      const aa = BigInt(a["16"] || "0");
      const bb = BigInt(b["16"] || "0");
      return aa < bb ? -1 : aa > bb ? 1 : 0;
    });
    return all;
  }

  function inferAsset(title) {
    if (/bitcoin/i.test(title || "")) return "BTC";
    if (/ethereum/i.test(title || "")) return "ETH";
    return "OTHER";
  }

  function inferDuration(slug, title) {
    if (/-15m-/.test(slug || "") || /15AM|30AM|45AM|15PM|30PM|45PM/.test(title || "")) return "15m";
    return "hourly_or_other";
  }

  function num(v) {
    const x = Number(v);
    return Number.isFinite(x) ? x : 0;
  }

  function csvEscape(v) {
    const s = String(v ?? "");
    return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  }

  function toCsv(rows, cols) {
    const lines = [cols.join(",")];
    for (const r of rows) lines.push(cols.map(c => csvEscape(r[c])).join(","));
    return lines.join("\n");
  }

  function download(name, text, type) {
    const blob = new Blob([text], { type });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 1500);
  }

  function summarize(fills) {
    const byOutcome = new Map();
    for (const f of fills) {
      const k = f.outcome_id;
      if (!byOutcome.has(k)) {
        byOutcome.set(k, {
          outcome_id: k,
          asset: f.asset,
          duration: f.duration,
          title: f.title,
          slug: f.slug,
          outcome: f.outcome,
          fills: 0,
          shares: 0,
          cost: 0,
          min_price: Infinity,
          max_price: -Infinity,
          first_timestamp: f.timestamp,
          last_timestamp: f.timestamp
        });
      }
      const s = byOutcome.get(k);
      s.fills += 1;
      s.shares += f.shares;
      s.cost += f.cost;
      s.min_price = Math.min(s.min_price, f.price);
      s.max_price = Math.max(s.max_price, f.price);
      if (f.timestamp < s.first_timestamp) s.first_timestamp = f.timestamp;
      if (f.timestamp > s.last_timestamp) s.last_timestamp = f.timestamp;
    }

    const outcomeSummary = [...byOutcome.values()].map(s => ({
      ...s,
      vwap: s.shares ? s.cost / s.shares : null,
      avg_shares_per_fill: s.fills ? s.shares / s.fills : null,
      min_price: Number.isFinite(s.min_price) ? s.min_price : null,
      max_price: Number.isFinite(s.max_price) ? s.max_price : null
    })).sort((a, b) => a.first_timestamp.localeCompare(b.first_timestamp));

    const byMarket = new Map();
    for (const s of outcomeSummary) {
      const key = s.slug || s.title || s.outcome_id;
      if (!byMarket.has(key)) byMarket.set(key, { slug: s.slug, title: s.title, asset: s.asset, duration: s.duration });
      const m = byMarket.get(key);
      const side = String(s.outcome || "").toLowerCase();
      if (side === "up") m.up = s;
      else if (side === "down") m.down = s;
    }

    const marketSummary = [...byMarket.values()].map(m => {
      const up = m.up || {};
      const down = m.down || {};
      const upShares = num(up.shares);
      const downShares = num(down.shares);
      const upVwap = up.vwap ?? null;
      const downVwap = down.vwap ?? null;
      const basis = (upVwap != null && downVwap != null) ? upVwap + downVwap : null;
      const matched = Math.min(upShares, downShares);
      return {
        asset: m.asset,
        duration: m.duration,
        title: m.title,
        slug: m.slug,
        up_fills: up.fills ?? 0,
        down_fills: down.fills ?? 0,
        total_fills: (up.fills ?? 0) + (down.fills ?? 0),
        up_shares: upShares,
        down_shares: downShares,
        matched_shares: matched,
        final_gap_shares: Math.abs(upShares - downShares),
        up_cost: num(up.cost),
        down_cost: num(down.cost),
        total_cost: num(up.cost) + num(down.cost),
        up_vwap: upVwap,
        down_vwap: downVwap,
        combined_basis: basis,
        gross_matched_edge: basis == null ? null : matched * (1 - basis),
        first_fill: [up.first_timestamp, down.first_timestamp].filter(Boolean).sort()[0] || "",
        last_fill: [up.last_timestamp, down.last_timestamp].filter(Boolean).sort().at(-1) || ""
      };
    }).sort((a, b) => a.first_fill.localeCompare(b.first_fill));

    return { outcomeSummary, marketSummary };
  }

  console.log("=== DATADASH FIRST-DAY FULL FINGERPRINT PULL ===");
  console.log(`WALLET      ${WALLET}`);
  console.log("MODE        BUY activity; raw rows preserved; no deduplication");

  const { firstDay, rows: aggregates } = await discoverFirstDayAggregates();
  const ids = [...new Set(aggregates.map(r => String(r["11"] || "")).filter(Boolean))];
  if (!ids.length) throw new Error("No first-day outcome IDs discovered.");

  console.log(`OUTCOMES    ${ids.length}`);
  const metadataRows = await fetchMetadata(ids);
  const meta = new Map(metadataRows.map(r => [String(r["1"] || ""), r]));

  const rawRows = await fetchRawFills(ids);
  const fills = rawRows.map((r, i) => {
    const outcomeId = String(r["11"] || "");
    const m = meta.get(outcomeId) || {};
    const title = String(m["6"] || "");
    const slug = String(m["7"] || m["21"] || "");
    return {
      n: i + 1,
      timestamp: String(r["2"] || ""),
      wallet: String(r["3"] || WALLET),
      asset: inferAsset(title),
      duration: inferDuration(slug, title),
      title,
      slug,
      outcome: String(m["2"] || ""),
      outcome_id: outcomeId,
      shares: num(r["5"]),
      price: num(r["6"]),
      cost: num(r["7"]),
      pnl: num(r["9"]),
      sequence: String(r["16"] || "")
    };
  });

  const { outcomeSummary, marketSummary } = summarize(fills);
  const stamp = firstDay.replaceAll("-", "");

  const bundle = {
    source: "DataDash ListTable",
    wallet: WALLET,
    first_day_datadash_timestamp_date: firstDay,
    note: "BUY rows only. Raw rows are preserved; no deduplication. Metadata is attached by DataDash outcome ID.",
    counts: {
      aggregated_outcome_rows: aggregates.length,
      outcome_ids: ids.length,
      raw_fill_rows: fills.length,
      markets: marketSummary.length
    },
    aggregated_rows: aggregates,
    metadata_rows: metadataRows,
    fills,
    outcome_summary: outcomeSummary,
    market_summary: marketSummary
  };

  download(`gabagool_first_day_${stamp}_full.json`, JSON.stringify(bundle, null, 2), "application/json");
  download(
    `gabagool_first_day_${stamp}_fills.csv`,
    toCsv(fills, ["n", "timestamp", "asset", "duration", "title", "slug", "outcome", "outcome_id", "shares", "price", "cost", "pnl", "sequence"]),
    "text/csv"
  );
  download(
    `gabagool_first_day_${stamp}_markets.csv`,
    toCsv(marketSummary, ["asset", "duration", "title", "slug", "up_fills", "down_fills", "total_fills", "up_shares", "down_shares", "matched_shares", "final_gap_shares", "up_cost", "down_cost", "total_cost", "up_vwap", "down_vwap", "combined_basis", "gross_matched_edge", "first_fill", "last_fill"]),
    "text/csv"
  );

  console.log("=== COMPLETE ===");
  console.log(`DAY         ${firstDay}`);
  console.log(`MARKETS     ${marketSummary.length}`);
  console.log(`OUTCOMES    ${ids.length}`);
  console.log(`RAW FILLS   ${fills.length}`);
  console.table(marketSummary.slice(0, 25));
  console.log("Downloaded:");
  console.log(`  gabagool_first_day_${stamp}_full.json`);
  console.log(`  gabagool_first_day_${stamp}_fills.csv`);
  console.log(`  gabagool_first_day_${stamp}_markets.csv`);
})();
