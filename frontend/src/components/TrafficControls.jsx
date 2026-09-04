import React, { useState } from 'react';
import { Play, Square, Zap, Sliders } from 'lucide-react';
import { startSimulation, stopSimulation } from '../services/api';

export function TrafficControls({ isSimulating, onStatusChange }) {
  const [selectedScenario, setSelectedScenario] = useState('FLASH_SALE');
  const [baseEps, setBaseEps] = useState(100);
  const [loading, setLoading] = useState(false);

  const scenarios = [
    { id: 'NORMAL', label: 'Normal Traffic', desc: 'Constant baseline EPS' },
    { id: 'GRADUAL_SPIKE', label: 'Gradual Spike', desc: 'Ramps up over 20s' },
    { id: 'FLASH_SALE', label: 'Flash Sale (Spike)', desc: '15x sudden burst load' },
    { id: 'RANDOM_BURST', label: 'Random Burst', desc: 'Poisson random micro-bursts' }
  ];

  async function handleStart() {
    setLoading(true);
    try {
      await startSimulation({
        scenario: selectedScenario,
        baseEps: Number(baseEps)
      });
      if (onStatusChange) onStatusChange();
    } catch (e) {
      alert('Error starting simulation');
    } finally {
      setLoading(false);
    }
  }

  async function handleStop() {
    setLoading(true);
    try {
      await stopSimulation();
      if (onStatusChange) onStatusChange();
    } catch (e) {
      alert('Error stopping simulation');
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="glass-panel" style={{ padding: '20px', marginBottom: '24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '16px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <Zap size={20} color="var(--accent-yellow)" />
          <h2 style={{ fontSize: '16px', fontWeight: '700' }}>Traffic Scenario Simulator</h2>
        </div>
        <div>
          {isSimulating ? (
            <button className="btn-danger" onClick={handleStop} disabled={loading}>
              <Square size={16} /> Stop Simulation
            </button>
          ) : (
            <button className="btn-primary" onClick={handleStart} disabled={loading}>
              <Play size={16} /> Trigger Traffic Surge
            </button>
          )}
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '12px', marginBottom: '16px' }}>
        {scenarios.map((sc) => (
          <div
            key={sc.id}
            onClick={() => setSelectedScenario(sc.id)}
            style={{
              padding: '12px',
              borderRadius: '10px',
              cursor: 'pointer',
              background: selectedScenario === sc.id ? 'rgba(6, 182, 212, 0.15)' : 'rgba(255, 255, 255, 0.03)',
              border: `1px solid ${selectedScenario === sc.id ? 'var(--accent-cyan)' : 'rgba(255, 255, 255, 0.05)'}`,
              transition: 'all 0.2s ease'
            }}
          >
            <div style={{ fontWeight: '600', fontSize: '14px', marginBottom: '4px', color: selectedScenario === sc.id ? 'var(--accent-cyan)' : 'var(--text-primary)' }}>
              {sc.label}
            </div>
            <div style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>{sc.desc}</div>
          </div>
        ))}
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
        <Sliders size={18} color="var(--text-muted)" />
        <span style={{ fontSize: '13px', color: 'var(--text-secondary)' }}>Base EPS Rate:</span>
        <input
          type="range"
          min="10"
          max="500"
          step="10"
          value={baseEps}
          onChange={(e) => setBaseEps(e.target.value)}
          style={{ flex: 1, accentColor: 'var(--accent-cyan)', cursor: 'pointer' }}
        />
        <span style={{ fontSize: '13px', fontWeight: '700', minWidth: '60px', fontFamily: 'var(--font-mono)' }}>
          {baseEps} events/s
        </span>
      </div>
    </div>
  );
}
