import React from 'react';
import { GitCommit, CheckCircle, Package, Clock, ShieldAlert } from 'lucide-react';

export function DecisionTimeline({ decisions = {} }) {
  const items = [
    { key: 'PROCESS', label: 'Critical Process', count: decisions.PROCESS || 0, icon: CheckCircle, color: 'var(--accent-green)' },
    { key: 'BATCH', label: 'Micro-Batched', count: decisions.BATCH || 0, icon: Package, color: 'var(--accent-cyan)' },
    { key: 'DEFER', label: 'Deferred Queue', count: decisions.DEFER || 0, icon: Clock, color: 'var(--accent-yellow)' },
    { key: 'SHED', label: 'Load Shed / DLQ', count: decisions.SHED || 0, icon: ShieldAlert, color: 'var(--accent-red)' }
  ];

  const total = Math.max(1, Object.values(decisions).reduce((a, b) => a + b, 0));

  return (
    <div className="glass-panel" style={{ padding: '20px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px' }}>
        <GitCommit size={18} color="var(--accent-blue)" />
        <h3 style={{ fontSize: '15px', fontWeight: '700' }}>HAEO Adaptive Decision Outcomes</h3>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: '12px' }}>
        {items.map((item) => {
          const Icon = item.icon;
          const pct = Math.round((item.count / total) * 100);
          return (
            <div
              key={item.key}
              style={{
                padding: '12px',
                borderRadius: '10px',
                background: 'rgba(255, 255, 255, 0.03)',
                border: '1px solid rgba(255, 255, 255, 0.05)',
                display: 'flex',
                flexDirection: 'column',
                gap: '8px'
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <Icon size={16} color={item.color} />
                <span style={{ fontSize: '12px', fontWeight: '600', color: 'var(--text-secondary)' }}>{item.label}</span>
              </div>

              <div style={{ fontSize: '20px', fontWeight: '800', fontFamily: 'var(--font-mono)', color: item.color }}>
                {item.count}
              </div>

              <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                {pct}% of routed events
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
