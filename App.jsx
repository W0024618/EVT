
//C:\Users\W0024618\Desktop\global-page\frontend\src\App.jsx

import React from 'react';
import { Routes, Route, Navigate, Link } from 'react-router-dom';
import { Box, AppBar, Toolbar, Button } from '@mui/material';

import GlobalPage   from './pages/GlobalPage.jsx';
import VipPage      from './pages/VipPage.jsx';
import ReportsPage  from './pages/ReportsPage.jsx';

export default function App() {
  return (
    <>
      <Routes>
        <Route path="/"      element={<GlobalPage />} />
        <Route path="/vip"   element={<VipPage />} />
        <Route path="/reports" element={<ReportsPage />} />
        <Route path="*"      element={<Navigate to="/" replace />} />
      </Routes>
    </>
  );
}
