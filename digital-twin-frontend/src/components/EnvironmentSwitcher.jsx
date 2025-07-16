// src/components/EnvironmentSwitcher.jsx
import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useEnvironment } from './EnvironmentContext';
import apiService from '../services/apiService';

function EnvironmentSwitcher() {
  const { environmentId: currentEnvironmentId } = useEnvironment();
  const navigate = useNavigate();
  const [environments, setEnvironments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchEnvironments = async () => {
      try {
        setLoading(true);
        // 调用后端 API 获取所有环境列表
        const data = await apiService.get('/environments', null); 
        setEnvironments(data);
      } catch (err) {
        console.error('Failed to fetch environments:', err);
        setError(err);
      } finally {
        setLoading(false);
      }
    };
    fetchEnvironments();
  }, []); // 组件挂载时获取一次

  const handleEnvironmentChange = (event) => {
    const newEnvId = event.target.value;
    if (newEnvId && newEnvId !== currentEnvironmentId) {
      // 将用户导航到新环境的仪表盘页面
      navigate(`/environments/${newEnvId}/dashboard`);
    }
  };

  if (loading) return <span>加载环境...</span>;
  if (error) return <span style={{ color: 'red' }}>加载环境失败</span>;

  return (
    <select 
      value={currentEnvironmentId || ''} 
      onChange={handleEnvironmentChange} 
      style={{ 
        padding: '8px', 
        borderRadius: '4px',
        border: '1px solid #007bff', // 添加蓝色边框
        backgroundColor: '#fff', // 白色背景
        color: '#007bff', // 蓝色字体
        fontSize: '1em',
        cursor: 'pointer',
      }}
    >
      <option value="" disabled>选择环境</option>
      {environments.map((env) => (
        <option key={env.environment_id} value={env.environment_id}>
          {/* 修改显示格式：环境名称 (环境ID) */}
          {env.display_name ? `${env.display_name} (${env.environment_id})` : env.environment_id}
        </option>
      ))}
    </select>
  );
}

export default EnvironmentSwitcher;