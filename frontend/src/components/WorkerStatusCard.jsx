import React from 'react';
import { Server } from 'lucide-react';

export function WorkerStatusCard({ utilization = {} }) {
  const workers = [
    { key: 'criticalWorker', label: 'Critical Worker', util: utilization.criticalWorker || 0.1, color: 'var(--accent-green)' },
    { key: 'batchWorker', label: 'Batch Worker', util: utilization.batchWorker || 0.1, color: 'var(--accent-cyan)' },
    { key: 'deferredWorker', label: 'Deferred Worker', util: utilization.deferredWorker || 0.1, color: 'var(--accent-yellow)' },
    { key: 'dlqWorker', label: 'DLQ / Audit Worker', util: utilization.dlqWorker || 0.0, color: 'var(--accent-red)' }
  ];

  return (
    <div className="glass-panel" style={{ padding: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px' }}>
        <Server size={18} color="var(--accent-green)" />
        <h3 style={{ fontSize: '15px', fontWeight: '700' }}>Worker Pool Utilization</h3>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '12px' }}>
        {workers.map((w) => {
          const pct = Math.round(w.util * 100);
          return (
            <div key={w.key} style={{ padding: '12px', background: 'rgba(255, 255, 255, 0.03)', borderRadius: '10px', border: '1px solid rgba(255, 255, 255, 0.05)' }}>
              <div style={{ fontSize: '12px', fontWeight: '600', color: 'var(--text-secondary)', marginBottom: '6px' }}>{w.label}</div>
              <div style={{ fontSize: '18px', fontWeight: '800', fontFamily: 'var(--font-mono)', color: w.color, marginBottom: '6px' }}>
                {pct}%
              </div>
              <div style={{ width: '100%', height: '6px', background: 'rgba(255, 255, 255, 0.05)', borderRadius: '3px', overflow: 'hidden' }}>
                <div style={{ width: `${pct}%`, height: '100%', background: w.color, borderRadius: '3px', transition: 'width 0.3s ease' }} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
