// src/App.jsx
import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import EnvironmentLayout from './components/EnvironmentLayout'; // 保持导入 EnvironmentLayout
import HomePage from './pages/HomePage';
import DashboardPage from './pages/DashboardPage';
import TwinsPage from './pages/TwinsPage';
import ModelsPage from './pages/ModelsPage';
import NotFoundPage from './pages/NotFoundPage';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<HomePage />} /> {/* 首页保持独立 */}

        {/* 环境相关的嵌套路由，由 EnvironmentLayout 包裹 */}
        <Route path="environments/:environment_id" element={<EnvironmentLayout />}>
          <Route index element={<Navigate to="dashboard" replace />} />
          <Route path="dashboard" element={<DashboardPage />} />
          <Route path="twins" element={<TwinsPage />} />
          <Route path="models" element={<ModelsPage />} />
          {/* ... 其他环境子路由 ... */}
          <Route path="*" element={<NotFoundPage />} /> {/* 环境内部的404 */}
        </Route>

        <Route path="*" element={<NotFoundPage />} /> {/* 任何未匹配的路径 */}
      </Routes>
    </Router>
  );
}

export default App;