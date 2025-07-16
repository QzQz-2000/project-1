// src/pages/TwinsPage.jsx
import React, { useEffect, useState } from 'react';
import { useEnvironment } from '../components/EnvironmentContext';
import apiService from '../services/apiService'; // 导入您创建的 apiService

function TwinsPage() {
  const { environmentId } = useEnvironment(); // 获取当前环境ID
  const [twins, setTwins] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchTwins = async () => {
      try {
        setLoading(true);
        setError(null); // 重置错误状态
        // 使用 apiService 发送请求，它会处理 environmentId 的注入
        const data = await apiService.get('/twins', environmentId);
        setTwins(data);
      } catch (err) {
        console.error('Failed to fetch twins:', err);
        setError(err); // 存储错误信息
      } finally {
        setLoading(false);
      }
    };

    // 确保 environmentId 可用才 fetchData
    if (environmentId) {
      fetchTwins();
    }
  }, [environmentId]); // 当 environmentId 改变时重新获取数据

  if (loading) {
    return (
      <div style={{ padding: '20px' }}>
        <h2 style={{ color: '#007bff' }}>加载数字孪生中...</h2> {/* 标题颜色改为蓝色 */}
        <p>环境 ID: {environmentId}</p>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: '20px', color: 'red' }}>
        <h2 style={{ color: '#dc3545' }}>加载数字孪生失败</h2> {/* 错误标题也保持红色 */}
        <p>错误信息: {error.message || JSON.stringify(error)}</p>
        <p>请检查后端服务和网络连接。</p>
      </div>
    );
  }

  return (
    <div style={{ padding: '20px' }}>
      <h1 style={{ color: '#007bff', marginBottom: '20px' }}>{/* 标题颜色改为蓝色，增加下边距 */}
        环境 "{environmentId}" 中的数字孪生
      </h1>
      {twins.length === 0 ? (
        <p style={{ color: '#555' }}>当前环境中没有数字孪生。</p>
      ) : (
        <ul style={{ listStyle: 'none', padding: 0 }}>
          {twins.map((twin) => (
            <li 
              key={twin.twin_id} 
              style={{ 
                marginBottom: '10px', 
                border: '1px solid #e0e0e0', 
                padding: '15px', 
                borderRadius: '8px',
                backgroundColor: '#f9f9f9',
                display: 'flex',
                flexDirection: 'column',
                gap: '8px',
              }}
            >
              <h3 style={{ color: '#007bff', margin: 0 }}>{twin.display_name || twin.twin_id}</h3>
              <p style={{ fontSize: '0.9em', color: '#666', margin: 0 }}>ID: {twin.twin_id}</p>
              {twin.description && <p style={{ fontSize: '0.9em', color: '#777', margin: 0 }}>描述: {twin.description}</p>}
              {/* 这里可以添加更多孪生信息 */}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default TwinsPage;