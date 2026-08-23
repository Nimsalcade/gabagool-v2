#!/usr/bin/env python3
"""Read-only Gabagool first-day collector using Polymarket's public Data API.

Defaults:
  wallet 0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d
  day    2025-10-29 America/New_York
  output Google Drive Desktop/My Drive/gabagool-first-day-2025-10-29

No wallet keys, signatures, orders, merges, or redeems are used.
"""
from __future__ import annotations
import argparse, csv, json, re, sys, time, urllib.error, urllib.parse, urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

API="https://data-api.polymarket.com"
WALLET="0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d"
SLUG=re.compile(r"^(?P<a>[a-z0-9]+)-updown-(?P<n>\d+)(?P<u>[mh])-(?P<t>\d{9,10})$")

def num(v):
    try:return float(v)
    except:return 0.0

def integer(v):
    try:return int(v)
    except:return 0

def iso(t):
    return datetime.fromtimestamp(int(t),timezone.utc).isoformat().replace("+00:00","Z") if t else ""

def get(path,params):
    url=f"{API}{path}?{urllib.parse.urlencode(params,doseq=True)}"
    for i in range(6):
        try:
            req=urllib.request.Request(url,headers={"User-Agent":"gabagool-forensics/1.0","Accept":"application/json"})
            with urllib.request.urlopen(req,timeout=45) as r:
                x=json.loads(r.read())
                if not isinstance(x,list): raise RuntimeError(x)
                return x
        except urllib.error.HTTPError as e:
            if e.code<500 and e.code!=429: raise
            err=e
        except Exception as e: err=e
        d=min(20,.8*(2**i)); print(f"RETRY {path}: {err}; {d:.1f}s",flush=True); time.sleep(d)
    raise RuntimeError(f"failed {url}: {err}")

def window(path,base,a,b,limit,cap):
    """Fetch integer-second [a,b); split if an endpoint offset cap is approached."""
    out=[]; off=0
    while True:
        p={**base,"start":a,"end":b-1,"limit":limit,"offset":off}
        pg=get(path,p); out+=pg
        if len(pg)<limit:return out
        if off+limit>cap:break
        off+=limit
    if b-a<=1: raise RuntimeError(f"too many {path} rows in second {a}")
    m=(a+b)//2
    print(f"SPLIT {path} {iso(a)} -> {iso(b)}",flush=True)
    return window(path,base,a,m,limit,cap)+window(path,base,m,b,limit,cap)

def day_fetch(path,base,a,b,limit,cap):
    out=[]; cur=a
    while cur<b:
        nxt=min(b,cur+3600); x=window(path,base,cur,nxt,limit,cap); out+=x
        print(f"FETCH {path:9s} {iso(cur)} -> {iso(nxt)} rows={len(x)}",flush=True); cur=nxt
    return out

def meta(slug):
    m=SLUG.match(slug or "")
    if not m:return {"asset_class":"","duration_s":None,"market_start":None,"market_end":None,"is_updown":False}
    dur=int(m["n"])*(3600 if m["u"]=="h" else 60); start=int(m["t"])
    return {"asset_class":m["a"].upper(),"duration_s":dur,"market_start":start,"market_end":start+dur,"is_updown":True}

def enrich(rows):
    z=[]
    for i,r0 in enumerate(rows,1):
        r=dict(r0); ts=integer(r.get("timestamp")); m=meta(str(r.get("slug") or ""))
        r.update(m); r["row_index"]=i; r["timestamp_iso_utc"]=iso(ts); r["notional_size_x_price"]=num(r.get("size"))*num(r.get("price"))
        if m["market_start"] is not None:
            r["market_start_iso_utc"]=iso(m["market_start"]); r["market_end_iso_utc"]=iso(m["market_end"])
            r["market_age_s"]=ts-m["market_start"]; r["market_remaining_s"]=m["market_end"]-ts
        z.append(r)
    return z

def side(r):return str(r.get("outcome") or "").strip().upper()

def inventory(trades):
    state=defaultdict(lambda:defaultdict(float)); out=[]
    for j,(_,r0) in enumerate(sorted(enumerate(trades),key=lambda x:(integer(x[1].get("timestamp")),x[0])),1):
        r=dict(r0); k=str(r.get("conditionId") or r.get("slug") or "?"); s=side(r); q=num(r.get("size")); p=num(r.get("price")); sd=str(r.get("side") or "").upper(); st=state[k]
        signed=q if sd=="BUY" else -q if sd=="SELL" else 0; st["net_"+s]+=signed
        if sd=="BUY":st["bq_"+s]+=q;st["bc_"+s]+=q*p
        up,down=st["net_UP"],st["net_DOWN"]; vu=st["bc_UP"]/st["bq_UP"] if st["bq_UP"] else None; vd=st["bc_DOWN"]/st["bq_DOWN"] if st["bq_DOWN"] else None
        r.update(chronological_index=j,running_net_up_shares=up,running_net_down_shares=down,running_gap_up_minus_down=up-down,running_abs_gap=abs(up-down),running_matched_net_shares=min(up,down) if up>=0 and down>=0 else None,running_buy_vwap_up=vu,running_buy_vwap_down=vd,running_buy_pair_vwap=(vu+vd if vu is not None and vd is not None else None)); out.append(r)
    return out

def summaries(trades,activity):
    tm=defaultdict(list); am=defaultdict(list)
    for r in trades:tm[str(r.get("conditionId") or r.get("slug") or "?")].append(r)
    for r in activity:am[str(r.get("conditionId") or r.get("slug") or "?")].append(r)
    out=[]
    for k,rs in tm.items():
        order=sorted(enumerate(rs),key=lambda x:(integer(x[1].get("timestamp")),x[0])); f=order[0][1]; m=meta(str(f.get("slug") or "")); row={"conditionId":f.get("conditionId"),"slug":f.get("slug"),"title":f.get("title"),**m,"trade_rows":len(rs),"first_trade_ts":integer(f.get("timestamp")),"last_trade_ts":integer(order[-1][1].get("timestamp"))}
        row["first_trade_iso_utc"]=iso(row["first_trade_ts"]);row["last_trade_iso_utc"]=iso(row["last_trade_ts"])
        if m["market_start"] is not None:row["first_trade_age_s"]=row["first_trade_ts"]-m["market_start"];row["last_trade_age_s"]=row["last_trade_ts"]-m["market_start"]
        running=defaultdict(float); maxgap=0
        for _,r in order:
            q=num(r.get("size")); sd=str(r.get("side") or "").upper(); s=side(r); running[s]+=q if sd=="BUY" else -q if sd=="SELL" else 0; maxgap=max(maxgap,abs(running["UP"]-running["DOWN"]))
        row["max_abs_trade_inventory_gap"]=maxgap;row["final_net_gap_up_minus_down"]=running["UP"]-running["DOWN"]
        for s in ("UP","DOWN"):
            buys=[r for r in rs if str(r.get("side") or "").upper()=="BUY" and side(r)==s]; sells=[r for r in rs if str(r.get("side") or "").upper()=="SELL" and side(r)==s]
            bq=sum(num(r.get("size")) for r in buys);bc=sum(num(r.get("size"))*num(r.get("price")) for r in buys);sq=sum(num(r.get("size")) for r in sells);sp=sum(num(r.get("size"))*num(r.get("price")) for r in sells); p=s.lower()
            row.update({f"{p}_buy_rows":len(buys),f"{p}_buy_shares":bq,f"{p}_buy_cost":bc,f"{p}_buy_vwap":bc/bq if bq else None,f"{p}_sell_rows":len(sells),f"{p}_sell_shares":sq,f"{p}_sell_proceeds":sp})
        matched=min(row["up_buy_shares"],row["down_buy_shares"]);row["matched_buy_shares"]=matched;vu=row["up_buy_vwap"];vd=row["down_buy_vwap"];row["buy_pair_vwap"]=(vu+vd if vu is not None and vd is not None else None);row["gross_matched_complete_set_edge"]=(matched*(1-row["buy_pair_vwap"]) if row["buy_pair_vwap"] is not None else None);row["final_buy_gap_up_minus_down"]=row["up_buy_shares"]-row["down_buy_shares"]
        c=defaultdict(int)
        for a in am[k]:c[str(a.get("type") or "UNKNOWN").upper()]+=1
        for typ in ("TRADE","SPLIT","MERGE","REDEEM","REWARD","CONVERSION","MAKER_REBATE","TAKER_REBATE","REFERRAL_REWARD"):row["activity_"+typ.lower()+"_rows"]=c[typ]
        out.append(row)
    return sorted(out,key=lambda r:(r["market_start"] is None,r["market_start"] or 0,str(r.get("slug") or "")))

def write_json(p,x):
    q=p.with_suffix(p.suffix+".part");q.write_text(json.dumps(x,indent=2,ensure_ascii=False)+"\n");q.replace(p)

def write_csv(p,rows):
    keys=[];seen=set()
    for r in rows:
        for k in r:
            if k not in seen:seen.add(k);keys.append(k)
    q=p.with_suffix(p.suffix+".part")
    with q.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows({k:(json.dumps(r.get(k),separators=(",",":")) if isinstance(r.get(k),(list,dict)) else r.get(k)) for k in keys} for r in rows)
    q.replace(p)

def drive(day):
    root=Path.home()/"Library"/"CloudStorage"; xs=[p/"My Drive" for p in sorted(root.glob("GoogleDrive-*")) if (p/"My Drive").is_dir()]
    if len(xs)!=1:raise RuntimeError("Expected exactly one Google Drive Desktop 'My Drive'. Use --output-dir if none or multiple are mounted.\n"+"\n".join(map(str,xs)))
    p=xs[0]/f"gabagool-first-day-{day}";p.mkdir(parents=True,exist_ok=True);return p

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--wallet",default=WALLET);ap.add_argument("--date",default="2025-10-29");ap.add_argument("--timezone",default="America/New_York");ap.add_argument("--output-dir",type=Path);a=ap.parse_args()
    if not re.fullmatch(r"0x[a-fA-F0-9]{40}",a.wallet):ap.error("bad wallet")
    d=date.fromisoformat(a.date);tz=ZoneInfo(a.timezone);beg=datetime(d.year,d.month,d.day,tzinfo=tz);end=beg+timedelta(days=1);lo,hi=int(beg.timestamp()),int(end.timestamp())
    try:out=a.output_dir.expanduser().resolve() if a.output_dir else drive(a.date)
    except Exception as e:print("ERROR",e,file=sys.stderr);return 2
    out.mkdir(parents=True,exist_ok=True);print(f"READ ONLY\nWALLET {a.wallet}\nDAY {a.date} {a.timezone}\nUTC {iso(lo)} -> {iso(hi)}\nOUTPUT {out}",flush=True)
    wallet=a.wallet.lower();tr=day_fetch("/trades",{"user":wallet,"takerOnly":"false"},lo,hi,10000,10000);ac=day_fetch("/activity",{"user":wallet,"sortBy":"TIMESTAMP","sortDirection":"ASC","excludeDepositsWithdrawals":"true"},lo,hi,500,5000)
    te,en=enrich(tr),enrich(ac);inv=inventory(te);mk=summaries(te,en);be=[r for r in inv if r["asset_class"] in {"BTC","ETH"} and r["is_updown"]];be15=[r for r in be if r["duration_s"]==900];bmk=[r for r in mk if r["asset_class"] in {"BTC","ETH"}]
    stem=f"gabagool_{a.date}_"; write_json(out/(stem+"trades_raw.json"),tr);write_json(out/(stem+"activity_raw.json"),ac);write_csv(out/(stem+"trades_enriched.csv"),te);write_csv(out/(stem+"activity_enriched.csv"),en);write_csv(out/(stem+"trade_inventory_chronological.csv"),inv);write_csv(out/(stem+"btc_eth_updown_fills.csv"),be);write_csv(out/(stem+"btc_eth_15m_fills.csv"),be15);write_csv(out/(stem+"market_summary.csv"),mk);write_csv(out/(stem+"btc_eth_market_summary.csv"),bmk)
    types=defaultdict(int)
    for r in ac:types[str(r.get("type") or "UNKNOWN").upper()]+=1
    summary={"wallet":wallet,"day":a.date,"timezone":a.timezone,"start_utc":iso(lo),"end_utc":iso(hi),"trades":len(tr),"activity":len(ac),"activity_by_type":dict(types),"markets":len(mk),"btc_eth_markets":len(bmk),"btc_eth_fills":len(be),"btc_eth_15m_fills":len(be15),"raw_rows_deduplicated":False,"output_dir":str(out)};write_json(out/(stem+"summary.json"),summary)
    print("=== COMPLETE ===");print(json.dumps(summary,indent=2));print("NEXT: wait for Drive sync, then upload summary.json + btc_eth_market_summary.csv here.");return 0
if __name__=="__main__":raise SystemExit(main())
