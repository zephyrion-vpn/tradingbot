export const DEFAULT_MARGIN_MODE = 'isolated';
export const SUPPORTED_MARGIN_MODES = new Set(['isolated', 'cross']);
export const DEFAULT_MAINTENANCE_RATE = 0.004;
export const DEFAULT_CLOSE_FEE_RATE = 0.0003;

export function normalizeMarginMode(mode) {
  const value = String(mode || DEFAULT_MARGIN_MODE).trim().toLowerCase();
  return SUPPORTED_MARGIN_MODES.has(value) ? value : DEFAULT_MARGIN_MODE;
}

export function normalizePair(pair) {
  if (!pair) return '';
  return String(pair).trim().replace('/', '-').toUpperCase();
}

export function toNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

export function getPositionMarkPrice(position, priceMap = {}) {
  const pair = normalizePair(position?.pair);
  const price = priceMap[pair];
  if (Number.isFinite(Number(price)) && Number(price) > 0) return Number(price);
  return toNumber(position?.entry_price ?? position?.entryPrice, 0);
}

export function calculatePositionPnl(position, markPrice) {
  const entry = toNumber(position?.entry_price ?? position?.entryPrice, 0);
  const size = toNumber(position?.size, 0);
  const type = String(position?.type || 'LONG').toUpperCase();
  const current = toNumber(markPrice, entry);
  if (entry <= 0 || size <= 0) {
    return { pnl: 0, pnlPct: 0, roe: 0 };
  }

  let diff = (current - entry) / entry;
  if (type === 'SHORT') diff = -diff;

  const pnl = diff * size;
  const pnlPct = diff * 100;
  const margin = toNumber(position?.margin, 0);
  const roe = margin > 0 ? (pnl / margin) * 100 : 0;
  return { pnl, pnlPct, roe };
}

export function calculateMaintenanceRequirement(size, maintenanceRate = DEFAULT_MAINTENANCE_RATE, closeFeeRate = DEFAULT_CLOSE_FEE_RATE) {
  const notional = toNumber(size, 0);
  if (notional <= 0) return 0;
  return notional * toNumber(maintenanceRate, DEFAULT_MAINTENANCE_RATE);
}

export function calculateCloseFee(size, closeFeeRate = DEFAULT_CLOSE_FEE_RATE) {
  return toNumber(size, 0) * toNumber(closeFeeRate, DEFAULT_CLOSE_FEE_RATE);
}

export function buildAccountSnapshot({
  balance = 0,
  positions = [],
  priceMap = {},
  maintenanceRate = DEFAULT_MAINTENANCE_RATE,
  closeFeeRate = DEFAULT_CLOSE_FEE_RATE,
}) {
  const normalizedPositions = Array.isArray(positions) ? positions : [];
  const rows = normalizedPositions.map((position) => {
    const markPrice = getPositionMarkPrice(position, priceMap);
    const { pnl, pnlPct, roe } = calculatePositionPnl(position, markPrice);
    const size = toNumber(position?.size, 0);
    const margin = toNumber(position?.margin, 0);
    const maintenance = calculateMaintenanceRequirement(size, maintenanceRate, closeFeeRate);
    const closeFee = calculateCloseFee(size, closeFeeRate);
    return {
      ...position,
      markPrice,
      pnl,
      pnlPct,
      roe,
      maintenance,
      closeFee,
      size: toNumber(size, 0),
      margin: toNumber(margin, 0),
    };
  });

  const reservedMargin = rows.reduce((sum, row) => sum + row.margin, 0);
  const unrealizedPnl = rows.reduce((sum, row) => sum + row.pnl, 0);
  const maintenanceMargin = rows.reduce((sum, row) => sum + row.maintenance, 0);
  const closeFees = rows.reduce((sum, row) => sum + row.closeFee, 0);
  const totalNotional = rows.reduce((sum, row) => sum + row.size, 0);
  const accountBalance = toNumber(balance, 0);
  const equity = accountBalance + reservedMargin + unrealizedPnl;
  const availableMargin = Math.max(0, accountBalance + unrealizedPnl);
  const marginUsage = equity > 0 ? reservedMargin / equity : 1;
  const maintenanceUsage = equity > 0 ? (maintenanceMargin + closeFees) / equity : 1;
  const riskLevel = Math.max(0, Math.min(100, maintenanceUsage * 100));

  return {
    balance: accountBalance,
    reservedMargin,
    unrealizedPnl,
    maintenanceMargin,
    closeFees,
    totalNotional,
    equity,
    availableMargin,
    marginUsage,
    riskLevel,
    positions: rows,
  };
}

export function estimateCrossLiquidationPrice(position, snapshot) {
  const entry = toNumber(position?.entry_price ?? position?.entryPrice, 0);
  const size = toNumber(position?.size, 0);
  const margin = toNumber(position?.margin, 0);
  const type = String(position?.type || 'LONG').toUpperCase();
  if (entry <= 0 || size <= 0) return entry;

  const positions = Array.isArray(snapshot?.positions) ? snapshot.positions : [];
  const totalNotional = positions.reduce((sum, row) => sum + toNumber(row.size, 0), 0) || size;
  const maintenance = toNumber(snapshot?.maintenanceMargin, 0) + toNumber(snapshot?.closeFees, 0);
  const equity = toNumber(snapshot?.equity, 0);
  const buffer = Math.max(0, equity - maintenance);
  const weight = totalNotional > 0 ? size / totalNotional : 1;
  const bufferedLoss = margin + buffer * weight;
  const maintenanceShare = calculateMaintenanceRequirement(size) * weight;
  const closeFeeShare = calculateCloseFee(size) * weight;
  const allowedLoss = Math.max(0, bufferedLoss - maintenanceShare - closeFeeShare);
  const priceDelta = allowedLoss / size;

  if (type === 'SHORT') {
    return Math.max(0.00000001, entry * (1 + priceDelta));
  }
  return Math.max(0.00000001, entry * (1 - priceDelta));
}

export function buildPositionRiskView(position, snapshot, currentPrice) {
  const pnlData = calculatePositionPnl(position, currentPrice);
  const liqPrice = estimateCrossLiquidationPrice(position, snapshot);
  const entry = toNumber(position?.entry_price ?? position?.entryPrice, 0);
  const price = toNumber(currentPrice, entry);
  const distance = entry > 0 ? Math.abs(price - liqPrice) / entry : 1;
  const liqDistancePct = entry > 0 ? Math.max(0, Math.min(100, distance * 100)) : 0;
  const accountRisk = toNumber(snapshot?.riskLevel, 0);

  return {
    ...pnlData,
    liqPrice,
    liqDistancePct,
    accountRisk,
  };
}
