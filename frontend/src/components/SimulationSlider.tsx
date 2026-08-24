import { useState } from "react";
import { apiClient } from "../services/apiClient";

interface Result {
  increase_percent: number;
  projected_avg_congestion_index: number;
}

export default function SimulationSlider() {
  const [value, setValue] = useState(10);
  const [result, setResult] = useState<Result | null>(null);
  const [loading, setLoading] = useState(false);

  const run = async () => {
    setLoading(true);
    try {
      const data = (await apiClient.simulatePopulation(value)) as Result;
      setResult(data);
    } catch {
      setResult(null);
    } finally {
      setLoading(false);
    }
  };

  const zoneColor =
    result == null
      ? "#8b98a5"
      : result.projected_avg_congestion_index < 0.4
      ? "#3fb950"
      : result.projected_avg_congestion_index < 0.7
      ? "#d29922"
      : "#f85149";

  return (
    <div className="sim-slider">
      <label style={{ fontSize: 13, color: "#8b98a5" }}>
        Population Increase Forecast: <strong style={{ color: "#e6edf3" }}>{value}%</strong>
      </label>
      <input
        type="range"
        min={0}
        max={100}
        value={value}
        onChange={(e) => setValue(Number(e.target.value))}
        onMouseUp={run}
        onTouchEnd={run}
      />
      {result && (
        <div style={{ fontSize: 12, color: zoneColor }}>
          Projected congestion index: {result.projected_avg_congestion_index.toFixed(2)}
        </div>
      )}
      {loading && <div style={{ fontSize: 12, color: "#8b98a5" }}>Running simulation stub...</div>}
    </div>
  );
}
