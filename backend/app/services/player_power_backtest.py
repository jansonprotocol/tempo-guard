# backend/app/services/player_power_backtest.py

def get_historical_player_nudge(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
    blend_weight: float = PLAYER_POWER_BLEND,
) -> float:
    """
    Compute player-power support_delta nudge using point-in-time squad snapshots.
    Now with debug logging to trace why zero is returned.
    """
    print(f"\n[DEBUG] get_historical_player_nudge for {home_team} vs {away_team} on {match_date}")
    print(f"[DEBUG]   league_code={league_code}, blend_weight={blend_weight}")

    if blend_weight <= 0.0:
        print(f"[DEBUG]   → blend_weight <= 0, returning 0.0")
        return 0.0

    # For domestic matches, look up snapshots in the match's league
    # For intl matches (UCL etc), look up in each team's home league
    if league_code in INTL_LEAGUE_CODES:
        print(f"[DEBUG]   International competition, resolving home leagues")
        home_cfg = db.query(TeamConfig).filter_by(team=home_team).first()
        away_cfg = db.query(TeamConfig).filter_by(team=away_team).first()
        home_league = home_cfg.league_code if home_cfg else None
        away_league = away_cfg.league_code if away_cfg else None
        print(f"[DEBUG]     home_league={home_league}, away_league={away_league}")
    else:
        home_league = league_code
        away_league = league_code

    if not home_league or not away_league:
        print(f"[DEBUG]   → missing home/away league, returning 0.0")
        return 0.0

    home_power = get_historical_squad_power(db, home_team, home_league, match_date)
    away_power = get_historical_squad_power(db, away_team, away_league, match_date)

    print(f"[DEBUG]   home_power={home_power}, away_power={away_power}")

    if home_power is None or away_power is None:
        print(f"[DEBUG]   → one or both powers None, returning 0.0")
        return 0.0

    # Cross-league normalisation for international competitions
    if league_code in INTL_LEAGUE_CODES:
        print(f"[DEBUG]   Applying cross-league coefficients")
        home_lc = db.query(LeagueConfig).filter_by(league_code=home_league).first()
        away_lc = db.query(LeagueConfig).filter_by(league_code=away_league).first()
        if home_lc and home_lc.strength_coefficient:
            old_home = home_power
            home_power *= float(home_lc.strength_coefficient)
            print(f"[DEBUG]     home: {old_home} × {home_lc.strength_coefficient} = {home_power}")
        if away_lc and away_lc.strength_coefficient:
            old_away = away_power
            away_power *= float(away_lc.strength_coefficient)
            print(f"[DEBUG]     away: {old_away} × {away_lc.strength_coefficient} = {away_power}")

    power_delta = (home_power - away_power) / 100.0
    print(f"[DEBUG]   raw power_delta = {power_delta}")

    nudge = power_delta * blend_weight
    nudge = _clip(nudge, -PLAYER_POWER_MAX_EFFECT, PLAYER_POWER_MAX_EFFECT)

    print(f"[DEBUG]   final nudge = {nudge}")
    return round(nudge, 4)
