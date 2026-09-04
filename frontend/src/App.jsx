import React, { useState, useEffect } from 'react';
import { Header } from './components/Header';
import { TrafficControls } from './components/TrafficControls';
import { PressureGauge } from './components/PressureGauge';
import { QueueMetricsChart } from './components/QueueMetricsChart';
import { LatencyChart } from './components/LatencyChart';
import { DecisionTimeline } from './components/DecisionTimeline';
import { WorkerStatusCard } from './components/WorkerStatusCard';
import { useWebSocket } from './hooks/useWebSocket';
import { fetchSimulationStatus } from './services/api';

export function App() {
  const { metrics, isConnected } = useWebSocket('ws://localhost:8000/ws/metrics');
  const [simStatus, setSimStatus] = useState({ isRunning: false, currentEps: 0, totalGenerated: 0 });
  const [latencyHistory, setLatencyHistory] = useState([]);

  useEffect(() => {
    loadSimStatus();
    const interval = setInterval(loadSimStatus, 1500);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (metrics && typeof metrics.avgLatencyMs === 'number') {
      setLatencyHistory((prev) => {
        const next = [...prev, metrics.avgLatencyMs];
        return next.slice(-20);
      });
    }
  }, [metrics]);

  async function loadSimStatus() {
    const st = await fetchSimulationStatus();
    setSimStatus(st);
  }

  const pressureScore = metrics?.pressureScore || 0.15;
  const pressureLevel = metrics?.pressureLevel || 'LOW';
  const throughputEps = metrics?.throughputEps || simStatus.currentEps || 0;
  const totalDropped = metrics?.totalDropped || 0;

  return (
    <div style={{ maxWidth: '1400px', margin: '0 auto', padding: '24px' }}>
      <Header
        isConnected={isConnected}
        pressureLevel={pressureLevel}
        pressureScore={pressureScore}
      />

      <TrafficControls
        isSimulating={simStatus.isRunning}
        onStatusChange={loadSimStatus}
      />

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(12, 1fr)', gap: '20px', marginBottom: '24px' }}>
        <div style={{ gridColumn: 'span 4' }}>
          <PressureGauge
            score={pressureScore}
            level={pressureLevel}
            eps={throughputEps}
            dropped={totalDropped}
          />
        </div>

        <div style={{ gridColumn: 'span 8', display: 'flex', flexDirection: 'column', gap: '20px' }}>
          <QueueMetricsChart queues={metrics?.queues || {}} />
          <LatencyChart avgLatencyMs={metrics?.avgLatencyMs || 10} history={latencyHistory} />
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(12, 1fr)', gap: '20px' }}>
        <div style={{ gridColumn: 'span 7' }}>
          <DecisionTimeline decisions={metrics?.decisions || {}} />
        </div>
        <div style={{ gridColumn: 'span 5' }}>
          <WorkerStatusCard utilization={metrics?.workerUtilization || {}} />
        </div>
      </div>
    </div>
  );
}

export default App;
