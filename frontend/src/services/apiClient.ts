const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    throw new Error(`API error ${res.status} on ${path}`);
  }
  return res.json() as Promise<T>;
}

export const apiClient = {
  getLatestSnapshot: () => request("/api/v1/dashboard/latest"),
  getHeatmap: (type: "traffic" | "air" | "temperature") =>
    request(`/api/v1/dashboard/heatmap?type=${type}`),
  getHistory: (metric: "aqi" | "traffic" | "temperature", hours = 6) =>
    request(`/api/v1/dashboard/history?metric=${metric}&hours=${hours}`),
  getConnectorStatus: () => request("/api/v1/admin/connectors/status"),
  simulatePopulation: (increase_percent: number) =>
    request("/api/v1/dashboard/simulate/population", {
      method: "POST",
      body: JSON.stringify({ increase_percent }),
    }),
};

export { API_BASE_URL };
