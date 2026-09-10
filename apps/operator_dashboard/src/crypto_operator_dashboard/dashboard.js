// All charts use already-published evidence. Browser controls never rerun a study.
(() => {
  const byId = (id) => document.getElementById(id);
  const readState = () => { try { const saved = JSON.parse(sessionStorage.getItem('research-view') || '{}'); return saved && typeof saved === 'object' ? saved : {}; } catch { return {}; } };
  const state = readState();
  const save = () => { try { sessionStorage.setItem('research-view', JSON.stringify(state)); } catch { /* Storage may be disabled. */ } };
  document.querySelectorAll('details').forEach((detail, index) => {
    detail.open = Boolean(state.details?.[index]);
    detail.addEventListener('toggle', () => {
      state.details = {...state.details, [index]: detail.open}; save();
    });
  });
  const tabs = [...document.querySelectorAll('[role="tab"]')];
  const activate = (name, focus = false) => {
    if (!tabs.some(tab => tab.dataset.tab === name)) name = 'research';
    tabs.forEach(tab => {
      const active = tab.dataset.tab === name;
      tab.setAttribute('aria-selected', String(active)); tab.tabIndex = active ? 0 : -1;
      byId(tab.getAttribute('aria-controls')).hidden = !active;
      if (active && focus) tab.focus();
    });
    byId('study-controls').hidden = !['research', 'trades'].includes(name);
    state.tab = name; save();
  };
  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => activate(tab.dataset.tab));
    tab.addEventListener('keydown', event => {
      let next;
      if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
      if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length;
      if (event.key === 'Home') next = 0;
      if (event.key === 'End') next = tabs.length - 1;
      if (next !== undefined) { event.preventDefault(); activate(tabs[next].dataset.tab, true); }
    });
  });
  activate(state.tab || 'research');
  byId('refresh-dashboard').addEventListener('click', () => location.reload());
  document.querySelectorAll('[data-copy]').forEach(button => button.addEventListener('click', async () => {
    try { await navigator.clipboard.writeText(button.dataset.copy); button.textContent = 'Copied'; }
    catch { button.textContent = 'Select text to copy'; }
  }));
  const visualization = JSON.parse(byId('research-visualization').textContent || '{}');
  const candidates = visualization.candidates || {};
  const candidate = byId('research-candidate'), range = byId('research-range'), segment = byId('research-segment');
  if (!candidate || !Object.keys(candidates).length) return;
  const dollars = value => Number(value).toLocaleString('en-US', {style: 'currency', currency: 'USD', maximumFractionDigits: 2});
  const percent = value => `${Number(value).toFixed(2)}%`;
  const date = value => new Date(value).toISOString().replace('T', ' ').slice(0, 16) + ' UTC';
  const shortDate = value => new Date(value).toISOString().slice(5, 16).replace('T', ' ');
  const name = value => value.replace('sma-', 'SMA ').replace('-', '/');
  const svgNS = 'http://www.w3.org/2000/svg';
  const node = (tag, attrs = {}, text = '') => {
    const element = document.createElementNS(svgNS, tag);
    Object.entries(attrs).forEach(([key, value]) => element.setAttribute(key, value));
    if (text) element.textContent = text;
    return element;
  };
  const text = (svg, x, y, label, attrs = {}) => svg.append(node('text', {x, y, fill: '#a9b9ce', 'font-size': 12, ...attrs}, label));
  const empty = (svg, label) => { svg.replaceChildren(); svg.setAttribute('viewBox', '0 0 960 320'); text(svg, 30, 60, label); };
  const colors = ['#69b6ff', '#bb9af7', '#ffc96b', '#6ee7b7'];
  const tooltip = byId('chart-tooltip');
  const hover = (element, label) => {
    element.append(node('title', {}, label));
    element.setAttribute('tabindex', '0'); element.setAttribute('aria-label', label);
    const show = () => { tooltip.textContent = label; tooltip.hidden = false; };
    element.addEventListener('pointerenter', show); element.addEventListener('focus', show);
    element.addEventListener('pointerleave', () => { tooltip.hidden = true; });
    element.addEventListener('blur', () => { tooltip.hidden = true; });
  };
  const axes = (svg, points, value, zero = false) => {
    svg.replaceChildren(); svg.setAttribute('viewBox', '0 0 960 320');
    const times = points.map(point => Date.parse(point.time)), values = points.map(value);
    let min = zero ? 0 : Math.min(...values), max = Math.max(...values);
    if (min === max) { min -= 1; max += 1; }
    const start = Math.min(...times), end = Math.max(...times);
    const x = time => 90 + (Date.parse(time) - start) / Math.max(end - start, 60000) * 830;
    const y = v => 260 - (v - min) / (max - min) * 220;
    for (let i = 0; i <= 4; i++) {
      const v = min + (max - min) * i / 4;
      svg.append(node('line', {x1: 90, x2: 920, y1: y(v), y2: y(v), stroke: '#293a50'}));
      text(svg, 80, y(v) + 4, dollars(v), {'text-anchor': 'end'});
    }
    for (let i = 0; i <= 3; i++) {
      text(svg, 90 + i * 830 / 3, 290, shortDate(start + (end - start) * i / 3), {'text-anchor': i === 0 ? 'start' : i === 3 ? 'end' : 'middle'});
    }
    text(svg, 920, 312, 'Time (UTC)', {'text-anchor': 'end'});
    return {x, y};
  };
  const equityChart = segments => {
    const svg = byId('research-equity-chart');
    const points = segments.flatMap(item => item.chart?.equity_points || []);
    if (!points.length) return empty(svg, 'Account history is unavailable for this run.');
    const {x, y} = axes(svg, points, point => Number(point.equity), true);
    segments.forEach((item, index) => {
      const values = item.chart?.equity_points || [];
      if (!values.length) return;
      svg.append(node('path', {d: values.map((point, i) => `${i ? 'L' : 'M'}${x(point.time)},${y(Number(point.equity))}`).join(' '), stroke: colors[index % colors.length], 'stroke-width': 2.5, fill: 'none', class: 'equity-series'}));
      values.forEach(point => {
        const dot = node('circle', {cx: x(point.time), cy: y(Number(point.equity)), r: 3, fill: colors[index % colors.length]});
        hover(dot, `${date(point.time)} | Account ${dollars(point.equity)} | ${point.position || ''} | Drawdown ${percent(Number(point.drawdown || 0) * 100)}`); svg.append(dot);
      });
      text(svg, x(values[0].time), 24, `Segment ${item.segmentNumber} · reset`, {fill: colors[index % colors.length]});
    });
  };
  let trades = [], page = 0;
  const pageSize = 20;
  const filteredTrades = () => {
    const side = byId('trade-side').value;
    const query = byId('trade-search').value.trim().toLowerCase();
    return trades.filter(trade => (side === 'all' || trade.side === side) && `${trade.time} ${trade.side} ${trade.execution_price}`.toLowerCase().includes(query));
  };
  const tradeChart = shown => {
    const svg = byId('research-trades-chart');
    if (!shown.length) return empty(svg, 'No published trades match these filters.');
    const {x, y} = axes(svg, shown, trade => Number(trade.execution_price));
    shown.forEach(trade => {
      const cx = x(trade.time), cy = y(Number(trade.execution_price)), buy = trade.side === 'BUY';
      const marker = node('path', {
        d: buy ? `M${cx},${cy-7} l-6,12 h12 Z` : `M${cx},${cy+7} l-6,-12 h12 Z`,
        fill: buy ? '#6ee7b7' : '#ff8e9d', stroke: '#101b29', 'stroke-width': 1,
        class: 'trade-marker', 'data-side': trade.side,
      });
      hover(marker, `${trade.side} | ${date(trade.time)} | Price ${dollars(trade.execution_price)} | Fee ${dollars(trade.fee)} | Segment ${trade.segmentNumber}`);
      svg.append(marker);
    });
  };
  const renderTrades = () => {
    tooltip.hidden = true;
    const filtered = filteredTrades();
    tradeChart(filtered);
    page = Math.min(page, Math.max(0, Math.ceil(filtered.length / pageSize) - 1));
    const body = byId('trade-rows'); body.replaceChildren();
    filtered.slice(page * pageSize, (page + 1) * pageSize).forEach(trade => {
      const row = document.createElement('tr');
      [date(trade.time), trade.side, dollars(trade.execution_price), dollars(trade.fee), trade.segmentNumber].forEach((value, index) => {
        const cell = document.createElement('td'); cell.textContent = value;
        if (index === 1) cell.className = trade.side === 'BUY' ? 'buy' : 'sell';
        row.append(cell);
      });
      body.append(row);
    });
    byId('trade-page').textContent = filtered.length ? `${page * pageSize + 1}–${Math.min((page+1)*pageSize, filtered.length)} of ${filtered.length} published markers` : 'No trades to display';
    byId('trades-prev').disabled = page === 0;
    byId('trades-next').disabled = (page + 1) * pageSize >= filtered.length;
  };
  const comparisonChart = () => {
    const svg = byId('research-comparison-chart'); svg.replaceChildren();
    const entries = Object.entries(candidates).map(([id, periods]) => [id, periods[range.value]?.aggregate]).filter(([, a]) => a);
    svg.setAttribute('viewBox', `0 0 960 ${entries.length * 76 + 65}`);
    const maxReturn = Math.max(100, ...entries.map(([, a]) => Math.abs(Number(a.percentage_return))));
    const zero = 340, returnScale = 180 / maxReturn, drawStart = 640, drawWidth = 180;
    text(svg, 340, 18, 'Return (%) · losses ← 0 → gains', {'text-anchor': 'middle'});
    text(svg, drawStart, 18, 'Largest drop from a peak');
    entries.forEach(([id, a], index) => {
      const y = 52 + index * 76, ret = Number(a.percentage_return), dd = Number(a.maximum_drawdown) * 100;
      text(svg, 10, y + 5, name(id), {fill: '#e6edf7'});
      svg.append(node('line', {x1: zero, x2: zero, y1: y - 18, y2: y + 24, stroke: '#94a3b8'}));
      svg.append(node('rect', {x: zero + Math.min(0, ret * returnScale), y: y-10, width: Math.abs(ret * returnScale), height: 16, fill: ret < 0 ? '#ff8e9d' : '#6ee7b7', rx: 3}));
      text(svg, 340, y + 26, percent(ret), {'text-anchor': 'middle'});
      svg.append(node('rect', {x: drawStart, y: y-10, width: dd / 100 * drawWidth, height: 16, fill: '#ffc96b', rx: 3}));
      text(svg, 840, y+5, percent(dd));
    });
    // This study's selection rule is fixed at 20%; the chart shows that rule.
    const limit = drawStart + drawWidth * .2;
    svg.append(node('line', {x1: limit, x2: limit, y1: 32, y2: entries.length * 76 + 12, stroke: '#ffc96b', 'stroke-dasharray': '4 4'}));
    text(svg, drawStart, entries.length * 76 + 48, 'Dashed line: 20% drawdown limit');
  };
  const update = (resetSegment = false) => {
    const selected = candidates[candidate.value]?.[range.value];
    if (!selected) return;
    const wanted = resetSegment ? 'all' : (segment.value || state.segment || 'all');
    segment.replaceChildren(new Option('All segments', 'all'));
    selected.segments.forEach((item, index) => {
      const start = item.source_segment?.start || item.chart?.equity_points?.[0]?.time;
      segment.add(new Option(`Segment ${index + 1}${start ? ' · ' + date(start).slice(0, 10) : ''}`, String(index)));
    });
    segment.value = [...segment.options].some(option => option.value === wanted) ? wanted : 'all';
    const segments = selected.segments.map((item, index) => ({...item, segmentNumber: index + 1})).filter((_, index) => segment.value === 'all' || Number(segment.value) === index);
    equityChart(segments); comparisonChart();
    const a = selected.aggregate;
    byId('research-metrics').replaceChildren();
    [['Period return', percent(a.percentage_return)], ['Largest drawdown', percent(Number(a.maximum_drawdown)*100)], ['Simulated fills', Number(a.fill_count).toLocaleString()], ['Fees paid', dollars(a.total_fees)]].forEach(([label, value]) => {
      const card = document.createElement('div'); card.className = 'metric';
      const title = document.createElement('span'); title.textContent = label;
      const strong = document.createElement('strong'); strong.textContent = value;
      card.append(title, strong); byId('research-metrics').append(card);
    });
    byId('research-chart-summary').textContent = `Period totals: ${a.segment_count} independent account(s). Return is their average; fees and fills are summed. Chart shows ${segments.length} segment(s). Account value includes cash and the value of held BTC.`;
    trades = segments.flatMap(item => (item.chart?.trade_markers || []).map(trade => ({...trade, segmentNumber: item.segmentNumber}))).sort((a,b) => Date.parse(a.time)-Date.parse(b.time));
    const total = segments.reduce((sum,item) => sum + Number(item.chart?.trade_marker_total || 0), 0);
    byId('trade-coverage').textContent = `Showing ${trades.length.toLocaleString()} published trade markers of ${total.toLocaleString()} actual simulated fills in the selected segments. ${trades.length < total ? 'This saved study contains a sample, so some trades are unavailable here.' : 'All fills in these segments are shown.'} Each buy or sell counts as one fill.`;
    byId('equity-sampling').textContent = 'Account history is sampled (up to 240 points per segment). Hover a point for its value. Exact drawdown is shown above; brief changes can be absent from the line.';
    state.candidate = candidate.value; state.range = range.value; state.segment = segment.value; save();
    page = 0; renderTrades();
  };
  if (candidates[state.candidate]) candidate.value = state.candidate;
  range.value = state.range === 'train' ? 'train' : 'validation';
  byId('trade-side').value = ['BUY', 'SELL'].includes(state.side) ? state.side : 'all';
  byId('trade-search').value = state.search || '';
  candidate.addEventListener('change', () => update(true)); range.addEventListener('change', () => update(true));
  segment.addEventListener('change', () => update());
  byId('trade-side').addEventListener('change', () => { state.side = byId('trade-side').value; save(); page = 0; renderTrades(); });
  byId('trade-search').addEventListener('input', () => { state.search = byId('trade-search').value; save(); page = 0; renderTrades(); });
  byId('trades-prev').addEventListener('click', () => { page--; renderTrades(); });
  byId('trades-next').addEventListener('click', () => { page++; renderTrades(); });
  update();
})();
