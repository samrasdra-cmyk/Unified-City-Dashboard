"""
Population Impact & Congestion Forecasting Engine.

Uses ML-trained Random Forest modeling and spatial grid zoning to predict
congestion index across urban zones under population growth pressure.
"""

from app.core.config import get_settings
from app.models.schemas import PopulationSimulationResponse
from app.services.forecast_service import forecast_model

settings = get_settings()


def simulate_population_increase(
    increase_percent: float, current_avg_congestion_index: float
) -> PopulationSimulationResponse:
    # Predict overall city congestion index using ML model
    predicted_congestion = forecast_model.predict_congestion(pop_increase_pct=increase_percent)

    # Blend with current real-time snapshot baseline if available
    if current_avg_congestion_index > 0:
        projected = round(min(1.0, (0.5 * predicted_congestion) + (0.5 * (current_avg_congestion_index * (1 + increase_percent / 100)))), 3)
    else:
        projected = predicted_congestion

    # Generate spatial zone breakdown across the city grid
    lat0, lng0 = settings.CITY_CENTER_LAT, settings.CITY_CENTER_LNG
    r = settings.GRID_RADIUS_DEG
    features = []

    # Sub-divide into core, north, south, east, west zones
    zones_offsets = [
        ("Downtown Core", 0.0, 0.0, 1.15),
        ("North Corridor", r * 0.6, 0.0, 1.05),
        ("South Suburb", -r * 0.6, 0.0, 0.90),
        ("East Industrial", 0.0, r * 0.6, 0.85),
        ("West Residential", 0.0, -r * 0.6, 0.95),
    ]

    for name, d_lat, d_lng, weight in zones_offsets:
        zone_congestion = round(min(1.0, projected * weight), 3)
        if zone_congestion < 0.4:
            color = "green"
        elif zone_congestion < 0.7:
            color = "orange"
        else:
            color = "red"

        features.append({
            "type": "Feature",
            "properties": {
                "zone_name": name,
                "projected_congestion_index": zone_congestion,
                "color": color,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [round(lng0 + d_lng, 5), round(lat0 + d_lat, 5)],
            },
        })

    return PopulationSimulationResponse(
        increase_percent=increase_percent,
        projected_avg_congestion_index=projected,
        zones={"type": "FeatureCollection", "features": features},
    )
