"""
src/transformations/catalog.py
================================
Fonctions de transformation et validation du catalogue musical.
"""


def normalize_artist_name(name: str) -> str:
    """
    Normalise le nom d'un artiste :
    - Supprime les espaces en début/fin
    - Met en title case
    - Retourne None si l'entrée est None
    """
    if name is None:
        return None
    return name.strip().title()


def validate_track_schema(track: dict) -> list:
    """
    Valide le schéma d'un track.
    Retourne une liste d'erreurs (vide si valide).

    Règles :
    - Champs obligatoires : id, artist_id, title, duration_ms
    - duration_ms doit être > 0 et < 3_600_000 (1 heure max)
    """
    errors = []
    required = ["id", "artist_id", "title", "duration_ms"]

    for field in required:
        if field not in track:
            errors.append(f"Champ manquant : {field}")

    if "duration_ms" in track:
        if not isinstance(track["duration_ms"], (int, float)):
            errors.append("duration_ms doit être un nombre")
        elif track["duration_ms"] <= 0:
            errors.append("duration_ms doit être > 0")
        elif track["duration_ms"] > 3_600_000:
            errors.append("duration_ms dépasse 1 heure (3_600_000 ms)")

    return errors


def deduplicate_artists(artists: list) -> list:
    """
    Supprime les doublons d'artistes basés sur (name normalisé, label).
    Garde le premier occurrence trouvée.
    """
    seen = set()
    result = []

    for artist in artists:
        name_normalized = normalize_artist_name(artist.get("name", ""))
        label = artist.get("label", "")
        key = (name_normalized.lower(), label.lower()) if name_normalized else None

        if key and key not in seen:
            seen.add(key)
            result.append(artist)

    return result
