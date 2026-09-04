const SIMULATOR_API_BASE = 'http://localhost:8001/api/simulation';

export async function fetchScenarios() {
  try {
    const res = await fetch(`${SIMULATOR_API_BASE}/scenarios`);
    if (!res.ok) throw new Error('Failed to fetch scenarios');
    return await res.json();
  } catch (err) {
    console.error(err);
    return {};
  }
}

export async function fetchSimulationStatus() {
  try {
    const res = await fetch(`${SIMULATOR_API_BASE}/status`);
    if (!res.ok) throw new Error('Failed to fetch simulation status');
    return await res.json();
  } catch (err) {
    console.error(err);
    return { isRunning: false, currentEps: 0, totalGenerated: 0 };
  }
}

export async function startSimulation(config) {
  try {
    const res = await fetch(`${SIMULATOR_API_BASE}/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config)
    });
    if (!res.ok) throw new Error('Failed to start simulation');
    return await res.json();
  } catch (err) {
    console.error(err);
    throw err;
  }
}

export async function stopSimulation() {
  try {
    const res = await fetch(`${SIMULATOR_API_BASE}/stop`, {
      method: 'POST'
    });
    if (!res.ok) throw new Error('Failed to stop simulation');
    return await res.json();
  } catch (err) {
    console.error(err);
    throw err;
  }
}
