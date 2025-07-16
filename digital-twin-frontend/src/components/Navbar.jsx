import React from 'react';
import { Link } from 'react-router-dom';
import { useEnvironment } from './EnvironmentContext';
import EnvironmentSwitcher from './EnvironmentSwitcher';

function Navbar() {
  const { environmentId } = useEnvironment();

  return (
    <nav className="navbar-container">
      <div className="navbar-left">
        <Link to="/" className="navbar-logo">
          数字孪生平台
        </Link>
        {environmentId && (
          <span className="navbar-env-label">
            当前环境：<strong>{environmentId}</strong>
          </span>
        )}
      </div>

      <div className="navbar-right">
        {environmentId && <EnvironmentSwitcher />}
        <ul className="navbar-links">
          <li><Link to="/settings" className="navbar-link">设置</Link></li>
          <li><Link to="/logout" className="navbar-link">退出</Link></li>
        </ul>
      </div>
    </nav>
  );
}

export default Navbar;
