import React from 'react';
import { useEnvironment } from '../components/EnvironmentContext';

function DashboardPage() {
  const { environmentId } = useEnvironment();

  return (
    <div style={styles.page}>
      <h1 style={styles.title}>环境 "{environmentId}" 的仪表盘</h1>
      <p style={styles.description}>这里将展示环境概览、关键指标和快速访问链接。</p>

      <div style={styles.card}>
        <p style={styles.cardHint}>仪表盘内容将在这里填充...</p>

        <dl style={styles.statsList}>
          <div style={styles.statItem}>
            <dt style={styles.label}>活跃孪生数量：</dt>
            <dd style={styles.value}>--</dd>
          </div>
          <div style={styles.statItem}>
            <dt style={styles.label}>最新告警：</dt>
            <dd style={styles.value}>--</dd>
          </div>
          <div style={styles.statItem}>
            <dt style={styles.label}>数据吞吐量：</dt>
            <dd style={styles.value}>--</dd>
          </div>
        </dl>
      </div>
    </div>
  );
}

const styles = {
  page: {
    padding: '40px 24px',
    backgroundColor: '#f8fafc',
    minHeight: '100vh',
  },
  title: {
    color: '#1d4ed8',
    fontSize: '28px',
    fontWeight: 700,
    marginBottom: '12px',
  },
  description: {
    color: '#64748b',
    fontSize: '16px',
    marginBottom: '24px',
  },
  card: {
    backgroundColor: '#ffffff',
    borderRadius: '12px',
    boxShadow: '0 2px 12px rgba(0,0,0,0.05)',
    padding: '24px',
    minHeight: '180px',
  },
  cardHint: {
    color: '#94a3b8',
    marginBottom: '16px',
  },
  statsList: {
    margin: 0,
    padding: 0,
  },
  statItem: {
    display: 'flex',
    justifyContent: 'space-between',
    marginBottom: '12px',
  },
  label: {
    fontWeight: 500,
    color: '#334155',
  },
  value: {
    color: '#475569',
  },
};

export default DashboardPage;
