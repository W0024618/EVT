//C:\Users\W0024618\Desktop\global-page\backend\controllers\reportController.js
import { rawReport } from '../services/reportService.js';

const ALL_REGIONS = ['apac', 'emea', 'laca', 'namer'];

export async function rawReportHandler(req, res) {
  try {
    const regionRaw = (req.query.region || '').toString();
    const region = regionRaw.toLowerCase();
    const location = req.query.location ? req.query.location.toString().trim() : undefined;
    const startDateParam = req.query.startDate;
    const endDateParam = req.query.endDate;
    const admitFilter = req.query.admitFilter ? req.query.admitFilter.toString().toLowerCase() : 'all';

    if (!region) return res.status(400).json({ success: false, message: 'region required' });
    if (!startDateParam || !endDateParam) {
      return res.status(400).json({ success: false, message: 'startDate and endDate required' });
    }

    const startDate = new Date(startDateParam);
    const endDate = new Date(endDateParam);
    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      return res.status(400).json({ success: false, message: 'Invalid date format' });
    }

    // GLOBAL: query all regions and merge results
    if (region === 'global') {
      const promises = ALL_REGIONS.map(r => rawReport(r, { startDate, endDate, location, admitFilter }));
      const results = await Promise.all(promises);
      // results is array of recordsets -> flatten
      const merged = results.flat();
      // Optionally sort by LocaleMessageTime desc to keep consistent ordering
      merged.sort((a, b) => {
        const ta = a.LocaleMessageTime ? new Date(a.LocaleMessageTime).getTime() : 0;
        const tb = b.LocaleMessageTime ? new Date(b.LocaleMessageTime).getTime() : 0;
        return tb - ta;
      });
      return res.json({ success: true, data: merged });
    }

    // Normal single-region flow
    const rows = await rawReport(region, { startDate, endDate, location, admitFilter });
    return res.json({ success: true, data: rows });
  } catch (err) {
    console.error('rawReportHandler error', err);
    return res.status(500).json({ success: false, message: err.message || 'server error' });
  }
}

