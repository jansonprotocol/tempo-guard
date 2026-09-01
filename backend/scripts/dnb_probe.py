"""How much worse is a REAL draw-no-bet quote than the one we synthesised?

The +6.05% on 212 gated DNBs was settled at a price built from the best
home quote and the best away quote across every book:

    P_syn = (1/H_max + 1/A_max) / (1/H_max)      for DNB1

That assumes both legs are takeable at once and carries no DNB-market
margin of its own. A real single-book draw_no_bet price will be worse.
How much worse is the whole question, and it decides everything:

    ROI = hit x (P - 1) - loss,  with hit 82.1%, loss 7.1%, pushes 10.8%
    P = 1.160 (synthetic)  ->  +6.0%
    P = 1.114 (-4%)        ->  +2.2%
    P = 1.090 (-6%)        ->  +0.3%      the edge is gone

So this needs no historical data and no settlement — only today's board.
Fetch draw_no_bet for upcoming fixtures, rebuild the synthetic price from
the same event's h2h, and compare. One credit per event per market.

    python dnb_probe.py             dry run
    python dnb_probe.py --go 40     spend at most 40 credits
"""
import statistics as st
import sys
import urllib.error
from collections import defaultdict

sys.path.insert(0, '/home/user/tempo-guard/backend')
from scripts.odds_api import REGIONS, SPORT, _get

WANT = ["ENG-PL", "ENG-CH", "GER-BL", "ITA-SA", "ESP-LL", "FRA-L1",
        "NED-ED", "POR-PL", "BEL-PL", "TUR-SL", "GRE-SL", "SCO-PL"]
EXCH = ("Betfair", "Matchbook", "Smarkets", "Betdaq")


def events(code):
    sport = SPORT.get(code)
    if not sport:
        return []
    try:
        data, _h = _get(f"/sports/{sport}/events/")
    except Exception:
        return []
    return data or []


def odds(sport, eid, markets):
    try:
        return _get(f"/sports/{sport}/events/{eid}/odds/", regions=REGIONS,
                    markets=markets, oddsFormat="decimal")
    except urllib.error.HTTPError as e:
        return {"_err": f"{e.code} {e.read()[:160].decode('utf8','ignore')}"}, {}
    except Exception as exc:
        return {"_err": str(exc)}, {}


def main():
    budget = int(sys.argv[sys.argv.index("--go") + 1]) if "--go" in sys.argv else 0
    plan = []
    for code in WANT:
        ev = events(code)
        for e in ev[:4]:
            plan.append((code, SPORT[code], e))
    print(f"  {len(plan)} upcoming fixtures across {len(WANT)} leagues "
          f"(event lists are free)")
    if not budget:
        print("  dry run — pass --go N to spend")
        return

    spent = 0
    gaps = []
    rows = []
    nodnb = defaultdict(int)
    for code, sport, e in plan:
        if spent >= budget:
            break
        # both markets in ONE request: cost is markets x regions = 2
        data, hdr = odds(sport, e["id"], "h2h,draw_no_bet")
        spent += 2
        if isinstance(data, dict) and data.get("_err"):
            print(f"  {code:8} ERR {data['_err'][:100]}")
            continue
        H = A = None
        dnb_h = dnb_a = None
        nbooks = 0
        for b in (data or {}).get("bookmakers", []):
            if any(x in b["title"] for x in EXCH):
                continue
            for mk in b.get("markets", []):
                o = {x["name"]: x["price"] for x in mk.get("outcomes", [])}
                if mk["key"] == "h2h":
                    if e["home_team"] in o:
                        H = max(H or 0, o[e["home_team"]])
                    if e["away_team"] in o:
                        A = max(A or 0, o[e["away_team"]])
                elif mk["key"] == "draw_no_bet":
                    nbooks += 1
                    if e["home_team"] in o:
                        dnb_h = max(dnb_h or 0, o[e["home_team"]])
                    if e["away_team"] in o:
                        dnb_a = max(dnb_a or 0, o[e["away_team"]])
        if not (H and A):
            continue
        h_, a_ = 1 / H, 1 / A
        syn_h, syn_a = (h_ + a_) / h_, (h_ + a_) / a_
        if dnb_h is None and dnb_a is None:
            nodnb[code] += 1
            print(f"  {code:8} {e['home_team'][:20]:20} no DNB market "
                  f"(syn {syn_h:.3f}/{syn_a:.3f})")
            continue
        for side, syn, real in (("1", syn_h, dnb_h), ("2", syn_a, dnb_a)):
            if real:
                g = real / syn - 1
                gaps.append(g)
                rows.append((code, e["home_team"][:18], side, syn, real, g,
                             nbooks))
        left = hdr.get("x-requests-remaining", "?")
        print(f"  {code:8} {e['home_team'][:20]:20} {nbooks:2} books  "
              f"syn {syn_h:.3f}/{syn_a:.3f}  real "
              f"{(dnb_h or 0):.3f}/{(dnb_a or 0):.3f}   left {left}")

    print(f"\n  spent {spent} credits, {len(gaps)} comparable prices\n")
    if nodnb:
        print("  leagues with NO draw_no_bet market offered:")
        for k, v in nodnb.items():
            print(f"    {k}: {v} fixtures")
        print()
    if not gaps:
        print("  no comparable prices — the market is not carried, which is")
        print("  itself the answer: the synthetic price is unreachable.")
        return
    med = st.median(gaps)
    print(f"  real vs synthetic:  median {100*med:+.2f}%   "
          f"mean {100*st.mean(gaps):+.2f}%   "
          f"worst {100*min(gaps):+.2f}%   best {100*max(gaps):+.2f}%")
    print(f"  {sum(1 for g in gaps if g >= 0)} of {len(gaps)} real quotes "
          f"MATCH or BEAT the synthetic\n")
    # what that does to the measured edge
    hit, loss = 0.821, 0.071
    for lbl, p in (("synthetic 1.160", 1.160),
                   (f"real, median gap {100*med:+.1f}%", 1.160 * (1 + med))):
        print(f"    {lbl:32} P={p:.3f}  ->  ROI "
              f"{100*(hit*(p-1)-loss):+.2f}%")
    print("\n  break-even needs P >= "
          f"{1 + loss/hit:.3f}, i.e. a gap no worse than "
          f"{100*((1+loss/hit)/1.160-1):+.1f}%")


if __name__ == "__main__":
    main()
