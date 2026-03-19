def _parse_page(html: str, league_code: str = "") -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Parse FBref page, returns (schedule_df, standings_df).
    Both may be None if not found.
    """
    if "Just a moment" in html or len(html) < 5000:
        print("  Cloudflare blocked.")
        return None, None

    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception as e:
        print(f"  Parse error: {e}")
        return None, None

    if not tables:
        return None, None

    is_intl = league_code in ("UCL", "UEL", "UECL", "EC", "WC")

    # Helper to check if a table is a schedule (has date, home, away columns)
    def is_schedule_table(df):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in df.columns
            ]
        cols_lower = [str(c).lower() for c in df.columns]
        has_date = any('date' in c for c in cols_lower)
        has_home = any('home' in c for c in cols_lower)
        has_away = any('away' in c for c in cols_lower)
        return has_date and has_home and has_away

    # Helper to check if a table is a standings table
    def is_standings_table(df):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in df.columns
            ]
        cols_lower = [str(c).lower() for c in df.columns]
        has_rk = any(c in ['rk', 'rank'] for c in cols_lower)
        has_squad = any(c in ['squad', 'team'] for c in cols_lower)
        has_pts = any(c in ['pts', 'points'] for c in cols_lower)
        return has_rk and has_squad and has_pts

    # Helper to detect schedule by looking at row values (date pattern)
    def contains_dates(df, sample_rows=5):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in df.columns
            ]
        sample = df.head(sample_rows).astype(str)
        for _, row in sample.iterrows():
            for val in row:
                if re.search(r'\d{4}-\d{2}-\d{2}', val) or re.search(r'\d{2}/\d{2}/\d{4}', val):
                    return True
        return False

    schedule_df = None
    standings_df = None

    # First pass: identify all tables
    for df in tables:
        if is_schedule_table(df):
            schedule_df = df.dropna(how="all")
            print(f"  Found schedule table by column names with {len(schedule_df)} rows")
        elif is_standings_table(df):
            standings_df = df.dropna(how="all")
            print(f"  Found standings table with {len(standings_df)} rows")

    # If we didn't find schedule by column names, try date-based detection
    if schedule_df is None and not is_intl:
        candidates = []
        for idx, df in enumerate(tables):
            if df.shape[1] >= 3 and contains_dates(df) and not is_standings_table(df):
                print(f"  Table {idx+1} contains dates – candidate schedule table")
                candidates.append(df)
        if candidates:
            schedule_df = max(candidates, key=len)
            schedule_df = schedule_df.dropna(how="all")
            print(f"  Selected candidate schedule table with {len(schedule_df)} rows")

    # For international, merge all schedule tables
    if is_intl and schedule_df is None:
        schedule_tables = []
        for t in tables:
            if is_schedule_table(t):
                if isinstance(t.columns, pd.MultiIndex):
                    t.columns = [
                        " ".join(str(v) for v in col if str(v) != "nan").strip()
                        for col in t.columns
                    ]
                schedule_tables.append(t)

        if schedule_tables:
            merged_parts = []
            for t in schedule_tables:
                t = t.dropna(how="all").copy()
                if isinstance(t.columns, pd.MultiIndex):
                    t.columns = [
                        " ".join(str(v) for v in col if str(v) != "nan").strip()
                        for col in t.columns
                    ]
                cols_lower_map = {str(c).lower(): c for c in t.columns}

                round_col = cols_lower_map.get("round") or cols_lower_map.get("wk")
                if round_col:
                    vals = t[round_col].dropna().astype(str)
                    vals = vals[~vals.str.lower().isin(["nan", "", "round", "wk"])]
                    label = vals.mode()[0] if not vals.empty else None
                else:
                    label = None

                t["_round_raw"] = label
                merged_parts.append(t)

            schedule_df = pd.concat(merged_parts, ignore_index=True)
            print(f"  Merged {len(schedule_tables)} schedule tables → {len(schedule_df)} rows")

    # If still no schedule, fall back to largest non-standings table
    if schedule_df is None and not is_intl:
        non_standings = [df for df in tables if not is_standings_table(df)]
        if non_standings:
            schedule_df = max(non_standings, key=len)
            schedule_df = schedule_df.dropna(how="all")
            print(f"  Fallback: largest non-standings table with {len(schedule_df)} rows")

    return schedule_df, standings_df
