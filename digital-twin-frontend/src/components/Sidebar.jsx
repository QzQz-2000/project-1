import React from 'react';
import { NavLink } from 'react-router-dom';

function Sidebar({ currentEnvironmentId }) {
  // 确保侧边栏链接正确地包含当前环境ID
  const getLinkTo = (path) => `/environments/${currentEnvironmentId}/${path}`;

  return (
    <aside style={{ width: '220px', background: '#f4f4f4', padding: '20px', borderRight: '1px solid #ddd' }}>
      <nav>
        <ul style={{ listStyle: 'none', padding: 0 }}>
          <li style={{ marginBottom: '10px' }}>
            <NavLink
              to={getLinkTo('dashboard')}
              style={({ isActive }) => ({
                textDecoration: 'none',
                color: isActive ? '#007bff' : '#333',
                fontWeight: isActive ? 'bold' : 'normal',
              })}
            >
              仪表盘
            </NavLink>
          </li>
          <li style={{ marginBottom: '10px' }}>
            <NavLink
              to={getLinkTo('twins')}
              style={({ isActive }) => ({
                textDecoration: 'none',
                color: isActive ? '#007bff' : '#333',
                fontWeight: isActive ? 'bold' : 'normal',
              })}
            >
              数字孪生
            </NavLink>
          </li>
          <li style={{ marginBottom: '10px' }}>
            <NavLink
              to={getLinkTo('models')}
              style={({ isActive }) => ({
                textDecoration: 'none',
                color: isActive ? '#007bff' : '#333',
                fontWeight: isActive ? 'bold' : 'normal',
              })}
            >
              模型
            </NavLink>
          </li>
          {/* 添加更多导航项 */}
        </ul>
      </nav>
    </aside>
  );
}

export default Sidebar;