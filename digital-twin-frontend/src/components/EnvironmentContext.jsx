import React, { createContext, useContext } from 'react';

// 创建环境上下文
const EnvironmentContext = createContext(null);

/**
 * Custom hook to consume the environment context.
 * Throws an error if not used within an EnvironmentProvider.
 */
export const useEnvironment = () => {
  const context = useContext(EnvironmentContext);
  if (!context) {
    throw new Error('useEnvironment must be used within an EnvironmentProvider');
  }
  return context;
};

/**
 * Provider component for the environment context.
 * It passes the current environmentId down the component tree.
 */
export const EnvironmentProvider = ({ children, environmentId }) => {
  // 您可以在这里添加更多环境相关的数据或状态
  const value = {
    environmentId,
    // 例如：environmentDetails: { name: '...', description: '...' }
  };

  return (
    <EnvironmentContext.Provider value={value}>
      {children}
    </EnvironmentContext.Provider>
  );
};