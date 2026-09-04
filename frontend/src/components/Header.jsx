import React from 'react';
import { Activity, Cpu, Wifi, WifiOff } from 'lucide-react';

export function Header({ isConnected, pressureLevel, pressureScore }) {
  const levelClass = pressureLevel ? `badge-${pressureLevel.toLowerCase()}` : 'badge-low';

  return (
    <header className="glass-panel" style={{ padding: '16px 28px', marginBottom: '24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
        <div style={{ background: 'linear-gradient(135deg, #06b6d4, #3b82f6)', padding: '10px', borderRadius: '12px', display: 'flex' }}>
          <Activity size={26} color="white" />
        </div>
        <div>
          <h1 style={{ fontSize: '20px', fontWeight: '800', letterSpacing: '-0.5px' }}>AEOP Control Center</h1>
          <p style={{ fontSize: '13px', color: 'var(--text-secondary)' }}>Adaptive Event Orchestration Platform • Hybrid Routing Engine</p>
        </div>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: '20px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Cpu size={18} color="var(--accent-cyan)" />
          <span style={{ fontSize: '13px', color: 'var(--text-secondary)' }}>System Pressure:</span>
          <span className={levelClass} style={{ padding: '4px 10px', borderRadius: '20px', fontSize: '12px', fontWeight: '700' }}>
            {pressureLevel || 'LOW'} ({(pressureScore || 0).toFixed(2)})
          </span>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '6px 14px', borderRadius: '20px', background: isConnected ? 'rgba(16, 185, 129, 0.1)' : 'rgba(239, 68, 68, 0.1)', border: `1px solid ${isConnected ? 'rgba(16, 185, 129, 0.2)' : 'rgba(239, 68, 68, 0.2)'}` }}>
          {isConnected ? <Wifi size={16} color="#10b981" /> : <WifiOff size={16} color="#ef4444" />}
          <span style={{ fontSize: '12px', fontWeight: '600', color: isConnected ? '#10b981' : '#ef4444' }}>
            {isConnected ? 'LIVE TELEMETRY' : 'DISCONNECTED'}
          </span>
        </div>
      </div>
    </header>
  );
}
