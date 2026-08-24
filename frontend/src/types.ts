export interface DashboardSnapshot {
  avg_speed_kmh: number | null;
  avg_aqi: number | null;
  transit_on_time_pct: number | null;
  avg_waste_fill_pct: number | null;
  updated_at: string | null;
}

export interface ConnectorStatus {
  name: string;
  status: "GREEN" | "YELLOW" | "RED";
  last_success: string | null;
  consecutive_failures: number;
  using_fallback: boolean;
}

export interface HistoryPoint {
  timestamp: string;
  value: number;
}

export interface GeoJSONFeatureCollection {
  type: "FeatureCollection";
  features: Array<{
    type: "Feature";
    geometry: { type: string; coordinates: number[] } | null;
    properties: Record<string, unknown>;
  }>;
}
