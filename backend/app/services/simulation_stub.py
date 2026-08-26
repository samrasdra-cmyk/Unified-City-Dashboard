"""
Placeholder for the future 3D Digital Twin / Agent-Based Modeling engine.

For this MVP, population-increase forecasting is approximated by scaling
current average traffic congestion linearly with the requested population
increase percentage, and bucketing city grid cells into red/orange/green
zones. Replace this with a real agent-based traffic/mobility model later.
"""

from app.models.schemas import PopulationSimulationResponse


def simulate_population_increase(
    increase_percent: float, current_avg_congestion_index: float
) -> PopulationSimulationResponse:
    projected = min(current_avg_congestion_index * (1 + increase_percent / 100), 1.0)

    if projected < 0.4:
        zone_color = "green"
    elif projected < 0.7:
        zone_color = "orange"
    else:
        zone_color = "red"

    zones = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "projected_congestion_index": round(projected, 3),
                    "color": zone_color,
                },
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            }
        ],
    }

    return PopulationSimulationResponse(
        increase_percent=increase_percent,
        projected_avg_congestion_index=round(projected, 3),
        zones=zones,
    )
