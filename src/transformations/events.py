"""
src/transformations/events.py
================================
Fonctions de validation des événements d'écoute.
"""

from datetime import datetime, timezone


def is_valid_listening_event(event: dict) -> bool:
    """
    Valide un événement d'écoute.

    Règles :
    - Champs obligatoires : event_id, user_id, track_id, timestamp, duration_ms
    - duration_ms >= 5000 (< 5s = pattern bot)
    - timestamp ne doit pas être dans le futur
    - completed=False avec duration_ms < 5000 = pattern bot
    """
    required = {"event_id", "user_id", "track_id", "timestamp", "duration_ms"}

    # Vérifier les champs obligatoires
    if not required.issubset(event):
        return False

    # Vérifier duration_ms
    duration_ms = event.get("duration_ms", 0)
    if not isinstance(duration_ms, (int, float)) or duration_ms < 5000:
        return False

    # Vérifier le timestamp
    try:
        ts_raw = event["timestamp"]
        if isinstance(ts_raw, str):
            ts_raw = ts_raw.replace("Z", "+00:00")
            ts = datetime.fromisoformat(ts_raw)
        else:
            ts = ts_raw

        # Timestamp dans le futur = invalide
        now = datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts > now:
            return False

    except (ValueError, TypeError):
        return False

    return True
