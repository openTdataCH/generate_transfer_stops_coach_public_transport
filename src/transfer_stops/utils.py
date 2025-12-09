import math

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Berechnet die kürzeste Distanz zwischen zwei GPS-Koordinaten auf der Erdkugel.
        
    Die Haversine-Formel berücksichtigt die Erdkrümmung und berechnet die Luftlinie
    entlang der Erdoberfläche (Großkreis-Distanz). Benannt nach der Haversine-Funktion
    (hav(θ) = sin²(θ/2)), die numerisch stabiler ist als andere trigonometrische Formeln.
        
    Returns: Distanz in Metern
    """
    R = 6371000  # Erdradius in Metern
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi, delta_lambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def estimate_walking_minutes(dist_m, speed_kmh=5.0):
    return dist_m / (speed_kmh * 1000) * 60