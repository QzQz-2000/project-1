// src/pages/ModelsPage.jsx
import React, { useEffect, useState } from 'react';
import { useEnvironment } from '../components/EnvironmentContext';
import apiService from '../services/apiService';

function ModelsPage() {
  const { environmentId } = useEnvironment();
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchModels = async () => {
      try {
        setLoading(true);
        setError(null);
        // 假设您的后端有一个 /environments/:id/models API
        const data = await apiService.get('/models', environmentId);
        setModels(data);
      } catch (err) {
        console.error('Failed to fetch models:', err);
        setError(err);
      } finally {
        setLoading(false);
      }
    };

    if (environmentId) {
      fetchModels();
    }
  }, [environmentId]);

  if (loading) {
    return (
      <div style={{ padding: '20px' }}>
        <h2 style={{ color: '#007bff' }}>加载模型中...</h2> {/* 标题颜色改为蓝色 */}
        <p>环境 ID: {environmentId}</p>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: '20px', color: 'red' }}>
        <h2 style={{ color: '#dc3545' }}>加载模型失败</h2> {/* 错误标题也保持红色 */}
        <p>错误信息: {error.message || JSON.stringify(error)}</p>
      </div>
    );
  }

  return (
    <div style={{ padding: '20px' }}>
      <h1 style={{ color: '#007bff', marginBottom: '20px' }}>{/* 标题颜色改为蓝色，增加下边距 */}
        环境 "{environmentId}" 中的模型
      </h1>
      {models.length === 0 ? (
        <p style={{ color: '#555' }}>当前环境中没有模型。</p>
      ) : (
        <ul style={{ listStyle: 'none', padding: 0 }}>
          {models.map((model) => (
            <li 
              key={model.model_id} 
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
              <h3 style={{ color: '#007bff', margin: 0 }}>{model.display_name || model.model_id}</h3>
              <p style={{ fontSize: '0.9em', color: '#666', margin: 0 }}>ID: {model.model_id}</p>
              {model.description && <p style={{ fontSize: '0.9em', color: '#777', margin: 0 }}>描述: {model.description}</p>}
              {/* 这里可以添加更多模型信息 */}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export default ModelsPage;