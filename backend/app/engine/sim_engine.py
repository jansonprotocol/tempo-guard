from datetime import datetime
from sqlalchemy.orm import Session
from app.models.league_config import LeagueConfig


def run_simulation(db: Session, team_a: str, team_b: str, date: str, league_code: str):

    # ----------------------------------------------------------------------
    # Determine mode (FutureMatch vs Retrosim)
    # ----------------------------------------------------------------------
    match_date = datetime.fromisoformat(date)
    now = datetime.utcnow()

    mode = "futurematch" if match_date > now else "retrosim"

    # ----------------------------------------------------------------------
    # Load league configuration
    # ----------------------------------------------------------------------
    config = (
        db.query(LeagueConfig)
        .filter(LeagueConfig.league_code == league_code)
        .first()
    )

    if not config:
        return {
            "error": f"League config not found for {league_code}"
        }

    # ----------------------------------------------------------------------
    # Simple MVP logic:
    # Combine under/over biases and tempo
    # ----------------------------------------------------------------------
    score = (
        config.base_over_bias
        - config.base_under_bias
        + (config.tempo_factor - 1.0)
        + (config.aggression_level * 0.4)
        - (config.safety_mode * 0.2)
    )

    # ----------------------------------------------------------------------
    # Corridor selection
    # ----------------------------------------------------------------------
    if score > 0.5:
    corridor = "O2.5" if config.volatility > 0.4 else "O1.5"
    translated = corridor if corridor == "O1.5" else "O2.5 (LOW_CONF)"
    elif score > 0.1:
    corridor = "O1.5"
    translated = "O1.5"
    elif score > -0.2:
    corridor = "U2.5"
    translated = "U2.5"
    else:
    corridor = "U3.5/4.5" if config.volatility > 0.4 else "U3.5"
    translated = corridor

    # ----------------------------------------------------------------------
    # Confidence
    # ----------------------------------------------------------------------
    abs_score = abs(score)
    if abs_score >= 0.5:
        confidence = "HIGH"
    elif abs_score >= 0.2:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    # ----------------------------------------------------------------------
    # Narrative
    # ----------------------------------------------------------------------
    narrative = (
        f"{league_code}: tempo={config.tempo_factor}, "
        f"over_bias={config.base_over_bias}, under_bias={config.base_under_bias}, "
        f"score={round(score,3)} → {translated}"
    )

    return {
        "mode": mode,
        "corridor": corridor,
        "translated": translated,
        "confidence": confidence,
        "narrative": narrative
    }
