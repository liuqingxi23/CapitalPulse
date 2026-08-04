'use client'

// Single-page real-time sector capital-flow dashboard.

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type {
  BarSeriesOption,
  ECharts,
  EChartsOption,
  EffectScatterSeriesOption,
  LineSeriesOption,
  ScatterSeriesOption,
} from 'echarts'
import {
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  Clock3,
  Radio,
  TrendingDown,
  TrendingUp,
} from 'lucide-react'

type Flow = {
  sector_code: string
  sector_name: string
  source_time: number
  received_at?: string
  main_net: number
  small_net: number
  mid_net: number
  large_net: number
  super_large_net: number
  granularity?: 'realtime' | 'minute_backfill'
}

type Selection = {
  rank: number
  sector_code: string
  sector_name: string
  market_cap: number
}

type SectorSeries = {
  rank: number
  sector_code: string
  sector_name: string
  points: [number, number][]
}

type DetailPoint = [number, number, number, number, number, number]

type DetailSectorSeries = {
  rank: number
  sector_code: string
  sector_name: string
  points: DetailPoint[]
}

type DetailHistoryPage = {
  trade_date: string
  page: number
  page_size: number
  total_items: number
  total_pages: number
  series: DetailSectorSeries[]
}

type DailyPoint = [string, number, number, number, number, number]

type DailySectorSeries = {
  rank: number
  sector_code: string
  sector_name: string
  points: DailyPoint[]
}

type DailyHistoryData = {
  selection_date: string | null
  interval: '1d'
  value_type: 'daily_net'
  days: number
  page: number
  page_size: number
  total_items: number
  total_pages: number
  failed_codes: string[]
  refresh_failed_codes?: string[]
  series: DailySectorSeries[]
}

function mergeDailyHistory(
  current: DailyHistoryData | null,
  incoming: DailyHistoryData,
): DailyHistoryData {
  if (!current || current.selection_date !== incoming.selection_date) return incoming

  const previousByCode = new Map(
    current.series.map((series) => [series.sector_code, series]),
  )
  const series = incoming.series.map((nextSeries) => {
    const previous = previousByCode.get(nextSeries.sector_code)
    return nextSeries.points.length === 0 && previous?.points.length
      ? previous
      : nextSeries
  })

  return {
    ...incoming,
    failed_codes: series
      .filter((item) => item.points.length === 0)
      .map((item) => item.sector_code),
    series,
  }
}

type ChartMode = 'main' | 'detail' | 'daily'

type ServiceStatus = {
  market_status: 'preopen' | 'open' | 'lunch' | 'closed' | 'stale' | 'error'
  last_source_time: number | null
  last_received_at: string | null
  selected_count: number
  last_error: string | null
  poll_seconds: number
  backfill_status?: 'idle' | 'running' | 'complete' | 'error'
  backfill_inserted_points?: number
  backfill_error?: string | null
}

type HistoryData = {
  trade_date: string
  selection: Selection[]
  series: SectorSeries[]
  latest: Flow[]
  status: ServiceStatus
}

type SocketMessage =
  | { type: 'snapshot'; data: { selection: Selection[]; flows: Flow[]; status: ServiceStatus } }
  | { type: 'update'; data: { source_time: number; received_at: string; complete: boolean; flows: Flow[] } }
  | { type: 'status' | 'heartbeat'; data: ServiceStatus }
  | { type: 'history_backfill'; data: { trade_date: string; inserted_points: number } }

const EMPTY_STATUS: ServiceStatus = {
  market_status: 'closed',
  last_source_time: null,
  last_received_at: null,
  selected_count: 0,
  last_error: null,
  poll_seconds: 3,
}

const FLOW_METRICS = [
  ['main_net', '主力'],
  ['super_large_net', '超大单'],
  ['large_net', '大单'],
  ['mid_net', '中单'],
  ['small_net', '小单'],
] as const

const DETAIL_METRICS = [
  { key: 'main_net', label: '主力', pointIndex: 1, color: '#475569' },
  { key: 'super_large_net', label: '超大单', pointIndex: 2, color: '#dc2626' },
  { key: 'large_net', label: '大单', pointIndex: 3, color: '#f97316' },
  { key: 'mid_net', label: '中单', pointIndex: 4, color: '#2563eb' },
  { key: 'small_net', label: '小单', pointIndex: 5, color: '#16a34a' },
] as const

const DETAIL_PAGE_SIZE = 6

const POSITIVE = '#c9363e'
const NEGATIVE = '#16865b'
const NEUTRAL = '#64748b'
const MORNING_START_SECONDS = 9 * 3600 + 30 * 60
const MORNING_END_SECONDS = 11 * 3600 + 30 * 60
const AFTERNOON_START_SECONDS = 13 * 3600
const AFTERNOON_END_SECONDS = 15 * 3600
const HALF_DAY_SECONDS = 2 * 3600
const FULL_SESSION_SECONDS = 4 * 3600
const MAIN_LABEL_ANCHOR_SECONDS = FULL_SESSION_SECONDS + 20 * 60

const timeFormatter = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
})

function formatTime(timestamp?: number | null): string {
  if (!timestamp) return '--:--:--'
  return timeFormatter.format(new Date(timestamp * 1000))
}

function tradingTimeOffset(timestamp: number): number | null {
  // China Standard Time has no daylight-saving transition. Shift to CST and
  // read with UTC getters so the chart behaves identically in every browser.
  const cst = new Date((timestamp + 8 * 3600) * 1000)
  const seconds = cst.getUTCHours() * 3600
    + cst.getUTCMinutes() * 60
    + cst.getUTCSeconds()
  if (seconds >= MORNING_START_SECONDS && seconds <= MORNING_END_SECONDS) {
    return seconds - MORNING_START_SECONDS
  }
  if (seconds >= AFTERNOON_START_SECONDS && seconds <= AFTERNOON_END_SECONDS) {
    return HALF_DAY_SECONDS + seconds - AFTERNOON_START_SECONDS
  }
  return null
}

function tradingAxisLabel(value: number): string {
  const offset = Math.round(value)
  if (offset === HALF_DAY_SECONDS) return '11:30/13:00'
  const seconds = offset < HALF_DAY_SECONDS
    ? MORNING_START_SECONDS + offset
    : AFTERNOON_START_SECONDS + offset - HALF_DAY_SECONDS
  const hour = Math.floor(seconds / 3600)
  const minute = Math.floor((seconds % 3600) / 60)
  return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`
}

function chartAmount(value: unknown): number {
  if (Array.isArray(value)) return Number(value[1] ?? 0)
  return Number(value ?? 0)
}

function formatYi(value: number, digits = 2): string {
  return `${value >= 0 ? '+' : ''}${(value / 1e8).toFixed(digits)}亿`
}

function displayName(value: string): string {
  return value.replace(/(?:Ⅱ|II|ii)$/u, '')
}

function socketUrl(): string {
  if (process.env.NEXT_PUBLIC_SECTOR_FLOW_WS_URL) {
    return process.env.NEXT_PUBLIC_SECTOR_FLOW_WS_URL
  }
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${window.location.hostname}:8000/ws/sector-flow`
}

function statusLabel(status: ServiceStatus['market_status']): string {
  return {
    preopen: '盘前',
    open: '交易中',
    lunch: '午休',
    closed: '已休市',
    stale: '数据延迟',
    error: '采集异常',
  }[status]
}

function mergeHistory(
  current: HistoryData,
  flows: Flow[],
  selection?: Selection[],
): HistoryData {
  const nextSelection = selection?.length ? selection : current.selection
  const rankByCode = new Map(nextSelection.map((item) => [item.sector_code, item.rank]))
  const namesByCode = new Map(nextSelection.map((item) => [item.sector_code, item.sector_name]))
  const nextSeries = new Map(
    current.series.map((series) => [
      series.sector_code,
      { ...series, points: [...series.points] },
    ]),
  )
  const nextLatest = new Map(current.latest.map((flow) => [flow.sector_code, flow]))

  for (const flow of flows) {
    const rank = rankByCode.get(flow.sector_code)
    if (!rank) continue
    const series = nextSeries.get(flow.sector_code) ?? {
      rank,
      sector_code: flow.sector_code,
      sector_name: namesByCode.get(flow.sector_code) ?? flow.sector_name,
      points: [],
    }
    const lastPoint = series.points.at(-1)
    if (lastPoint?.[0] === flow.source_time) {
      lastPoint[1] = flow.main_net
    } else if (!lastPoint || flow.source_time > lastPoint[0]) {
      series.points.push([flow.source_time, flow.main_net])
    }
    nextSeries.set(flow.sector_code, series)
    nextLatest.set(flow.sector_code, flow)
  }

  return {
    ...current,
    selection: nextSelection,
    series: [...nextSeries.values()].sort((a, b) => a.rank - b.rank),
    latest: [...nextLatest.values()],
  }
}

function detailPointFromFlow(flow: Flow): DetailPoint {
  return [
    flow.source_time,
    flow.main_net,
    flow.super_large_net,
    flow.large_net,
    flow.mid_net,
    flow.small_net,
  ]
}

function mergeDetailHistoryPage(page: DetailHistoryPage, flows: Flow[]): DetailHistoryPage {
  const flowsByCode = new Map(flows.map((flow) => [flow.sector_code, flow]))
  if (!page.series.some((series) => flowsByCode.has(series.sector_code))) return page

  return {
    ...page,
    series: page.series.map((series) => {
      const flow = flowsByCode.get(series.sector_code)
      if (!flow) return series
      const points = [...series.points]
      const nextPoint = detailPointFromFlow(flow)
      const lastPoint = points.at(-1)
      if (lastPoint?.[0] === flow.source_time) {
        points[points.length - 1] = nextPoint
      } else if (!lastPoint || flow.source_time > lastPoint[0]) {
        points.push(nextPoint)
      }
      return { ...series, points }
    }),
  }
}

function MetricCard({
  title,
  value,
  detail,
  positive,
}: {
  title: string
  value: string
  detail: string
  positive?: boolean
}) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-sm dark:border-slate-800 dark:bg-slate-900">
      <p className="text-xs text-slate-500 dark:text-slate-400">{title}</p>
      <p className={`mt-1 font-mono text-xl font-semibold tabular-nums ${
        positive === undefined ? 'text-slate-900 dark:text-white' : positive ? 'text-red-600' : 'text-emerald-600'
      }`}>
        {value}
      </p>
      <p className="mt-0.5 truncate text-xs text-slate-400">{detail}</p>
    </div>
  )
}

function DetailSectorChart({
  sector,
  flashing,
}: {
  sector: DetailSectorSeries
  flashing: boolean
}) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const chartRef = useRef<ECharts | null>(null)

  const option = useMemo<EChartsOption>(() => {
    const chartSeries: Array<LineSeriesOption | ScatterSeriesOption | EffectScatterSeriesOption> = []

    DETAIL_METRICS.forEach((metric, metricIndex) => {
      const values = sector.points.flatMap((point) => {
        const offset = tradingTimeOffset(point[0])
        return offset === null ? [] : [[offset, point[metric.pointIndex] / 1e8, point[0]]]
      })
      chartSeries.push({
        id: `${sector.sector_code}-${metric.key}`,
        name: metric.label,
        type: 'line',
        data: values,
        encode: { x: 0, y: 1 },
        showSymbol: false,
        sampling: 'lttb',
        animationDurationUpdate: 300,
        lineStyle: { width: metric.key === 'main_net' ? 2.2 : 1.5, color: metric.color, opacity: 0.9 },
        itemStyle: { color: metric.color },
        emphasis: { focus: 'series', lineStyle: { width: 3 } },
        markLine: metricIndex === 0 ? {
          symbol: 'none',
          silent: true,
          lineStyle: { color: '#94a3b8', width: 1, opacity: 0.65 },
          label: { show: false },
          data: [{ yAxis: 0 }],
        } : undefined,
      })

      const endpoint = values.at(-1)
      if (!endpoint) return
      chartSeries.push({
        id: `${sector.sector_code}-${metric.key}-endpoint`,
        name: `${metric.label} endpoint`,
        type: 'scatter',
        data: [endpoint],
        encode: { x: 0, y: 1 },
        symbol: 'circle',
        symbolSize: 6,
        silent: true,
        tooltip: { show: false },
        z: 4,
        itemStyle: { color: metric.color, opacity: 0.65, borderWidth: 0 },
      })
      if (flashing) {
        chartSeries.push({
          id: `${sector.sector_code}-${metric.key}-endpoint-flash`,
          name: `${metric.label} update`,
          type: 'effectScatter',
          data: [endpoint],
          encode: { x: 0, y: 1 },
          symbol: 'circle',
          symbolSize: 6,
          silent: true,
          tooltip: { show: false },
          z: 5,
          itemStyle: { color: metric.color, opacity: 0.25 },
          rippleEffect: { period: 1.2, scale: 1.8, brushType: 'stroke' },
        })
      }
    })

    return {
      animation: true,
      animationDuration: 0,
      animationDurationUpdate: 300,
      grid: { left: 44, right: 18, top: 18, bottom: 36, containLabel: true },
      tooltip: {
        trigger: 'axis',
        confine: true,
        renderMode: 'richText',
        formatter: (params: unknown) => {
          const items = (Array.isArray(params) ? params : [params]) as Array<{
            seriesName?: string
            value?: unknown
          }>
          const firstValue = items[0]?.value
          const sourceTime = Array.isArray(firstValue) ? Number(firstValue[2] ?? 0) : 0
          return [
            sourceTime ? formatTime(sourceTime) : '--:--:--',
            ...items.map((item) => {
              const amount = chartAmount(item.value)
              return `${item.seriesName ?? ''}  ${amount >= 0 ? '+' : ''}${amount.toFixed(2)}亿元`
            }),
          ].join('\n')
        },
      },
      xAxis: {
        type: 'value',
        min: 0,
        max: FULL_SESSION_SECONDS,
        interval: 2 * 3600,
        axisLabel: {
          color: '#94a3b8',
          fontSize: 10,
          hideOverlap: true,
          formatter: (value: number) => tradingAxisLabel(value),
        },
        axisLine: { lineStyle: { color: '#cbd5e1' } },
        splitLine: { show: false },
      },
      yAxis: {
        type: 'value',
        name: '净流入（亿元）',
        nameLocation: 'middle',
        nameRotate: 90,
        nameGap: 38,
        nameTextStyle: { color: '#94a3b8', fontSize: 10 },
        axisLabel: { color: '#94a3b8', fontSize: 10 },
        splitLine: { lineStyle: { color: '#e2e8f0', opacity: 0.45 } },
      },
      series: chartSeries,
    }
  }, [flashing, sector])

  useEffect(() => {
    let disposed = false
    let observer: ResizeObserver | null = null
    void import('echarts').then((echarts) => {
      if (disposed || !containerRef.current) return
      chartRef.current = echarts.init(containerRef.current, undefined, { renderer: 'canvas' })
      chartRef.current.setOption(option, { notMerge: true, lazyUpdate: true })
      observer = new ResizeObserver(() => chartRef.current?.resize())
      observer.observe(containerRef.current)
    })
    return () => {
      disposed = true
      observer?.disconnect()
      chartRef.current?.dispose()
      chartRef.current = null
    }
  }, [])

  useEffect(() => {
    chartRef.current?.setOption(option, { notMerge: true, lazyUpdate: true })
  }, [option])

  return (
    <article className="flex min-h-0 flex-col overflow-hidden rounded-xl border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="flex items-center justify-between border-b border-slate-100 px-3 py-2 dark:border-slate-800">
        <h3 className="truncate text-sm font-medium">{sector.rank}. {displayName(sector.sector_name)}</h3>
        <span className="font-mono text-[11px] text-slate-400">{sector.sector_code}</span>
      </div>
      <div ref={containerRef} className="h-[250px] w-full xl:min-h-0 xl:flex-1" aria-label={`${displayName(sector.sector_name)}细分资金流向曲线`} />
    </article>
  )
}

function DailySectorChart({ sector }: { sector: DailySectorSeries }) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const chartRef = useRef<ECharts | null>(null)

  const option = useMemo<EChartsOption>(() => {
    const values = sector.points.map((point) => [point[0], point[1] / 1e8])
    const lastDateIndex = values.length - 1
    const dateLabelStep = Math.max(1, Math.ceil(values.length / 6))
    const barSeries: BarSeriesOption = {
      id: `daily-${sector.sector_code}-main`,
      name: '主力',
      type: 'bar',
      data: values,
      encode: { x: 0, y: 1 },
      barMaxWidth: 14,
      barMinHeight: 1,
      animationDurationUpdate: 300,
      itemStyle: {
        color: (params) => {
          const amount = chartAmount(params.value)
          return amount > 0 ? POSITIVE : amount < 0 ? NEGATIVE : NEUTRAL
        },
      },
      emphasis: { focus: 'series' },
      markLine: {
        symbol: 'none',
        silent: true,
        lineStyle: { color: '#94a3b8', width: 1, opacity: 0.75 },
        label: { show: false },
        data: [{ yAxis: 0 }],
      },
    }

    return {
      animation: true,
      animationDuration: 0,
      animationDurationUpdate: 300,
      grid: { left: 44, right: 18, top: 18, bottom: 36, containLabel: true },
      tooltip: {
        trigger: 'axis',
        confine: true,
        renderMode: 'richText',
        formatter: (params: unknown) => {
          const items = (Array.isArray(params) ? params : [params]) as Array<{
            seriesName?: string
            value?: unknown
          }>
          const firstValue = items[0]?.value
          const tradeDate = Array.isArray(firstValue) ? String(firstValue[0] ?? '') : ''
          return [
            tradeDate || '--',
            ...items.map((item) => {
              const amount = chartAmount(item.value)
              return `${item.seriesName ?? ''}  ${amount >= 0 ? '+' : ''}${amount.toFixed(2)}亿元`
            }),
          ].join('\n')
        },
      },
      xAxis: {
        type: 'category',
        boundaryGap: true,
        axisLabel: {
          color: '#94a3b8',
          hideOverlap: false,
          showMinLabel: true,
          showMaxLabel: true,
          interval: (index: number) => (
            index === 0
            || index === lastDateIndex
            || index % dateLabelStep === 0
          ),
          formatter: (value: string) => value.slice(5),
        },
        axisLine: { lineStyle: { color: '#cbd5e1' } },
        splitLine: { show: false },
      },
      yAxis: {
        type: 'value',
        name: '主力净流入（亿元）',
        nameLocation: 'middle',
        nameRotate: 90,
        nameGap: 38,
        nameTextStyle: { color: '#94a3b8', fontSize: 10 },
        axisLabel: { color: '#94a3b8', fontSize: 10 },
        splitLine: { lineStyle: { color: '#e2e8f0', opacity: 0.45 } },
      },
      series: [barSeries],
    }
  }, [sector])

  useEffect(() => {
    let disposed = false
    let observer: ResizeObserver | null = null
    void import('echarts').then((echarts) => {
      if (disposed || !containerRef.current) return
      chartRef.current = echarts.init(containerRef.current, undefined, { renderer: 'canvas' })
      chartRef.current.setOption(option, { notMerge: true, lazyUpdate: true })
      observer = new ResizeObserver(() => chartRef.current?.resize())
      observer.observe(containerRef.current)
    })
    return () => {
      disposed = true
      observer?.disconnect()
      chartRef.current?.dispose()
      chartRef.current = null
    }
  }, [])

  useEffect(() => {
    chartRef.current?.setOption(option, { notMerge: true, lazyUpdate: true })
  }, [option])

  return (
    <article className="flex min-h-0 flex-col overflow-hidden rounded-xl border border-slate-200 bg-white dark:border-slate-800 dark:bg-slate-900">
      <div className="flex items-center justify-between border-b border-slate-100 px-3 py-2 dark:border-slate-800">
        <h3 className="truncate text-sm font-medium">{sector.rank}. {displayName(sector.sector_name)}</h3>
        <span className="font-mono text-[11px] text-slate-400">{sector.sector_code}</span>
      </div>
      <div ref={containerRef} className="h-[250px] w-full xl:min-h-0 xl:flex-1" aria-label={`${displayName(sector.sector_name)}最近30日主力资金柱状图`} />
    </article>
  )
}

export default function SectorFlowPage() {
  const chartContainer = useRef<HTMLDivElement | null>(null)
  const chartRef = useRef<ECharts | null>(null)
  const chartOptionRef = useRef<EChartsOption>({})
  const socketRef = useRef<WebSocket | null>(null)
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const endpointFlashTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const reconnectAttempts = useRef(0)
  const latestFlowsRef = useRef<Flow[]>([])
  const [history, setHistory] = useState<HistoryData>({
    trade_date: '',
    selection: [],
    series: [],
    latest: [],
    status: EMPTY_STATUS,
  })
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [flashingEndpoints, setFlashingEndpoints] = useState<Set<string>>(() => new Set())
  const [chartMode, setChartMode] = useState<ChartMode>('main')
  const [detailPage, setDetailPage] = useState(1)
  const [detailPages, setDetailPages] = useState<Record<number, DetailHistoryPage>>({})
  const [detailLoadingPage, setDetailLoadingPage] = useState<number | null>(null)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [dailyPage, setDailyPage] = useState(1)
  const [dailyPages, setDailyPages] = useState<Record<number, DailyHistoryData>>({})
  const [dailyLoadingPage, setDailyLoadingPage] = useState<number | null>(null)
  const [dailyError, setDailyError] = useState<string | null>(null)

  const fetchHistory = useCallback(async () => {
    try {
      const response = await fetch('/api/finance/sector-flow/history?top=30', {
        cache: 'no-store',
      })
      const payload = await response.json()
      if (!response.ok || payload.code !== 200 || !payload.data) {
        throw new Error(payload.msg || '历史数据加载失败')
      }
      setHistory(payload.data as HistoryData)
      latestFlowsRef.current = (payload.data as HistoryData).latest
      setDetailPages({})
      setDetailError(null)
      setDailyPages({})
      setDailyError(null)
      setLoadError(null)
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : '历史数据加载失败')
    } finally {
      setLoading(false)
    }
  }, [])

  const flashUpdatedEndpoints = useCallback((flows: Flow[]) => {
    setFlashingEndpoints(new Set(flows.map((flow) => flow.sector_code)))
    if (endpointFlashTimer.current) clearTimeout(endpointFlashTimer.current)
    endpointFlashTimer.current = setTimeout(() => {
      setFlashingEndpoints(new Set())
      endpointFlashTimer.current = null
    }, 1200)
  }, [])

  const connectSocket = useCallback(() => {
    if (socketRef.current?.readyState === WebSocket.OPEN) return
    const socket = new WebSocket(socketUrl())
    socketRef.current = socket

    socket.onopen = () => {
      reconnectAttempts.current = 0
      void fetchHistory()
    }
    socket.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data) as SocketMessage
        if (message.type === 'snapshot') {
          setDetailPages((pages) => Object.fromEntries(
            Object.entries(pages).map(([page, data]) => [page, mergeDetailHistoryPage(data, message.data.flows)]),
          ))
          setHistory((current) => ({
            ...mergeHistory(current, message.data.flows, message.data.selection),
            status: message.data.status,
          }))
        } else if (message.type === 'update') {
          flashUpdatedEndpoints(message.data.flows)
          setDetailPages((pages) => Object.fromEntries(
            Object.entries(pages).map(([page, data]) => [page, mergeDetailHistoryPage(data, message.data.flows)]),
          ))
          setHistory((current) => ({
            ...mergeHistory(current, message.data.flows),
            status: {
              ...current.status,
              market_status: 'open',
              last_source_time: message.data.source_time,
              last_received_at: message.data.received_at,
              last_error: null,
            },
          }))
        } else if (message.type === 'history_backfill') {
          void fetchHistory()
        } else if (message.type === 'status' || message.type === 'heartbeat') {
          setHistory((current) => ({ ...current, status: message.data }))
        }
      } catch {
        // Ignore malformed upstream messages; the next valid snapshot recovers state.
      }
    }
    socket.onclose = () => {
      socketRef.current = null
      const delay = Math.min(1000 * 2 ** reconnectAttempts.current, 15000)
      reconnectAttempts.current += 1
      reconnectTimer.current = setTimeout(connectSocket, delay)
    }
    socket.onerror = () => socket.close()
  }, [fetchHistory, flashUpdatedEndpoints])

  useEffect(() => {
    void fetchHistory()
    connectSocket()
    return () => {
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current)
      if (endpointFlashTimer.current) clearTimeout(endpointFlashTimer.current)
      const socket = socketRef.current
      socketRef.current = null
      if (socket) {
        socket.onclose = null
        socket.close()
      }
    }
  }, [connectSocket, fetchHistory])

  useEffect(() => {
    latestFlowsRef.current = history.latest
  }, [history.latest])

  useEffect(() => {
    if (chartMode !== 'detail' || detailPages[detailPage]) return
    const controller = new AbortController()
    const requestedPage = detailPage
    setDetailLoadingPage(requestedPage)
    setDetailError(null)

    void fetch(`/api/finance/sector-flow/detail-history?page=${requestedPage}`, {
      cache: 'no-store',
      signal: controller.signal,
    })
      .then(async (response) => {
        const payload = await response.json()
        if (!response.ok || payload.code !== 200 || !payload.data) {
          throw new Error(payload.msg || '行业细分历史加载失败')
        }
        const data = mergeDetailHistoryPage(
          payload.data as DetailHistoryPage,
          latestFlowsRef.current,
        )
        setDetailPages((pages) => ({ ...pages, [requestedPage]: data }))
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) return
        setDetailError(error instanceof Error ? error.message : '行业细分历史加载失败')
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setDetailLoadingPage((page) => page === requestedPage ? null : page)
        }
      })

    return () => controller.abort()
  }, [chartMode, detailPage, detailPages])

  useEffect(() => {
    if (chartMode !== 'daily' || dailyPages[dailyPage]) return
    const controller = new AbortController()
    const requestedPage = dailyPage
    setDailyLoadingPage(requestedPage)
    setDailyError(null)

    void fetch(
      `/api/finance/sector-flow/daily-history?top=30&days=30&page=${requestedPage}`,
      { cache: 'no-store', signal: controller.signal },
    )
      .then(async (response) => {
        const payload = await response.json()
        if (!response.ok || payload.code !== 200 || !payload.data) {
          throw new Error(payload.msg || '30日日频资金数据加载失败')
        }
        const data = payload.data as DailyHistoryData
        setDailyPages((pages) => ({
          ...pages,
          [requestedPage]: mergeDailyHistory(pages[requestedPage] ?? null, data),
        }))
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) return
        setDailyError(error instanceof Error ? error.message : '30日日频资金数据加载失败')
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setDailyLoadingPage((page) => page === requestedPage ? null : page)
        }
      })

    return () => controller.abort()
  }, [chartMode, dailyPage, dailyPages])

  useEffect(() => {
    let disposed = false
    void import('echarts').then((echarts) => {
      if (disposed || !chartContainer.current) return
      chartRef.current = echarts.init(chartContainer.current, undefined, { renderer: 'canvas' })
      chartRef.current.setOption(chartOptionRef.current, { notMerge: true, lazyUpdate: true })
    })
    const resize = () => chartRef.current?.resize()
    window.addEventListener('resize', resize)
    return () => {
      disposed = true
      window.removeEventListener('resize', resize)
      chartRef.current?.dispose()
      chartRef.current = null
    }
  }, [])

  const visibleSelection = history.selection
  const visibleCodes = useMemo(
    () => new Set(visibleSelection.map((item) => item.sector_code)),
    [visibleSelection],
  )
  const visibleLatest = useMemo(
    () => history.latest.filter((item) => visibleCodes.has(item.sector_code)),
    [history.latest, visibleCodes],
  )
  const latestByCode = useMemo(
    () => new Map(visibleLatest.map((item) => [item.sector_code, item])),
    [visibleLatest],
  )

  const chartOption = useMemo<EChartsOption>(() => {
    const visibleSeries = history.series.filter((item) => visibleCodes.has(item.sector_code))
    const series: Array<LineSeriesOption | ScatterSeriesOption | EffectScatterSeriesOption> = []
    const prepared = visibleSeries.map((item) => {
      const values = item.points.flatMap(([timestamp, value]) => {
        const offset = tradingTimeOffset(timestamp)
        return offset === null ? [] : [[offset, value / 1e8, timestamp]]
      })
      const latest = latestByCode.get(item.sector_code)?.main_net ?? item.points.at(-1)?.[1] ?? 0
      return { item, values, latest, endpoint: values.at(-1) }
    })
    const plottedAmounts = prepared.flatMap(({ values }) => (
      values.map((value) => Number(value[1]))
    ))
    const dataMin = Math.min(0, ...plottedAmounts)
    const dataMax = Math.max(0, ...plottedAmounts)
    const dataSpan = Math.max(
      dataMax - dataMin,
      Math.abs(dataMax) * 0.15,
      Math.abs(dataMin) * 0.15,
      1,
    )
    const yMin = dataMin - dataSpan * 0.08
    const yMax = dataMax + dataSpan * 0.08
    const labelSpan = yMax - yMin
    const orderedLabels = prepared
      .filter((entry) => entry.endpoint)
      .sort((a, b) => b.latest - a.latest || a.item.rank - b.item.rank)
    const labelYByCode = new Map(orderedLabels.map((entry, index) => [
      entry.item.sector_code,
      orderedLabels.length <= 1
        ? (yMin + yMax) / 2
        : yMax - labelSpan * (0.025 + (index / (orderedLabels.length - 1)) * 0.95),
    ]))

    prepared.forEach(({ item, values, latest, endpoint }) => {
      const color = latest > 0 ? POSITIVE : latest < 0 ? NEGATIVE : NEUTRAL
      series.push({
        id: item.sector_code,
        name: displayName(item.sector_name),
        type: 'line',
        data: values,
        encode: { x: 0, y: 1 },
        showSymbol: false,
        sampling: 'lttb',
        animationDurationUpdate: 300,
        lineStyle: { width: Math.abs(latest) > 1e9 ? 2 : 1.2, color, opacity: 0.82 },
        itemStyle: { color },
        emphasis: { focus: 'series', lineStyle: { width: 3 } },
      })

      if (!endpoint) return

      const labelY = labelYByCode.get(item.sector_code) ?? Number(endpoint[1])
      series.push({
        id: `${item.sector_code}-label-connector`,
        name: `${displayName(item.sector_name)} label connector`,
        type: 'line',
        data: [endpoint, [MAIN_LABEL_ANCHOR_SECONDS, labelY]],
        encode: { x: 0, y: 1 },
        showSymbol: true,
        symbol: 'circle',
        symbolSize: 4,
        silent: true,
        tooltip: { show: false },
        animationDurationUpdate: 300,
        z: 2,
        lineStyle: { width: 1, color, opacity: 0.34 },
        itemStyle: { color, opacity: 0.72 },
        endLabel: {
          show: true,
          distance: 7,
          align: 'left',
          verticalAlign: 'middle',
          color,
          fontSize: 12,
          fontWeight: 600,
          formatter: () => `${displayName(item.sector_name)}  ${formatYi(latest)}`,
        },
        labelLayout: { hideOverlap: false },
        emphasis: { disabled: true },
      })

      series.push({
        id: `${item.sector_code}-endpoint`,
        name: `${displayName(item.sector_name)} endpoint`,
        type: 'scatter',
        data: [endpoint],
        encode: { x: 0, y: 1 },
        symbol: 'circle',
        symbolSize: 6,
        silent: true,
        tooltip: { show: false },
        z: 4,
        itemStyle: { color, opacity: 0.65, borderWidth: 0 },
      })

      if (flashingEndpoints.has(item.sector_code)) {
        series.push({
          id: `${item.sector_code}-endpoint-flash`,
          name: `${displayName(item.sector_name)} endpoint update`,
          type: 'effectScatter',
          data: [endpoint],
          encode: { x: 0, y: 1 },
          symbol: 'circle',
          symbolSize: 6,
          silent: true,
          tooltip: { show: false },
          z: 5,
          itemStyle: { color, opacity: 0.25 },
          rippleEffect: { period: 1.2, scale: 1.8, brushType: 'stroke' },
        })
      }
    })

    return {
      animation: true,
      animationDuration: 0,
      animationDurationUpdate: 300,
      grid: { left: 60, right: 158, top: 32, bottom: 38, containLabel: true },
      tooltip: {
        trigger: 'axis',
        confine: true,
        renderMode: 'richText',
        formatter: (params: unknown) => {
          const items = (Array.isArray(params) ? params : [params]) as Array<{
            seriesName?: string
            value?: unknown
          }>
          const firstValue = items[0]?.value
          const sourceTime = Array.isArray(firstValue) ? Number(firstValue[2] ?? 0) : 0
          return [
            sourceTime ? formatTime(sourceTime) : '--:--:--',
            ...items.map((item) => {
              const amount = chartAmount(item.value)
              return `${item.seriesName ?? ''}  ${amount >= 0 ? '+' : ''}${amount.toFixed(2)}亿元`
            }),
          ].join('\n')
        },
      },
      xAxis: {
        type: 'value',
        min: 0,
        max: MAIN_LABEL_ANCHOR_SECONDS,
        interval: 30 * 60,
        axisLabel: {
          color: '#94a3b8',
          hideOverlap: true,
          showMinLabel: true,
          showMaxLabel: true,
          formatter: (value: number) => (
            value > FULL_SESSION_SECONDS ? '' : tradingAxisLabel(value)
          ),
        },
        axisLine: { lineStyle: { color: '#cbd5e1' } },
        splitLine: { show: false },
      },
      yAxis: {
        type: 'value',
        min: yMin,
        max: yMax,
        name: '主力净流入（亿元）',
        nameLocation: 'middle',
        nameRotate: 90,
        nameGap: 48,
        nameTextStyle: { color: '#94a3b8' },
        axisLabel: { color: '#94a3b8', formatter: (value: number) => value.toFixed(1) },
        splitLine: { lineStyle: { color: '#e2e8f0', opacity: 0.55 } },
      },
      series,
    }
  }, [flashingEndpoints, history.series, latestByCode, visibleCodes])

  useEffect(() => {
    chartOptionRef.current = chartOption
    chartRef.current?.setOption(chartOption, { notMerge: true, lazyUpdate: true })
  }, [chartOption])

  useEffect(() => {
    if (chartMode !== 'main') return
    const frame = requestAnimationFrame(() => chartRef.current?.resize())
    return () => cancelAnimationFrame(frame)
  }, [chartMode])

  const aggregates = useMemo(() => {
    return Object.fromEntries(FLOW_METRICS.map(([key]) => [
      key,
      visibleLatest.reduce((sum, flow) => sum + flow[key], 0),
    ])) as Record<(typeof FLOW_METRICS)[number][0], number>
  }, [visibleLatest])

  const inflowCount = visibleLatest.filter((flow) => flow.main_net > 0).length
  const outflowCount = visibleLatest.filter((flow) => flow.main_net < 0).length
  const leader = [...visibleLatest].sort((a, b) => b.main_net - a.main_net)[0]
  const activeDetailPage = detailPages[detailPage]
  const detailTotalPages = Math.max(
    1,
    activeDetailPage?.total_pages
      ?? Math.ceil((history.selection.length || 30) / DETAIL_PAGE_SIZE),
  )
  const detailTotalItems = activeDetailPage?.total_items ?? history.selection.length
  const detailRangeStart = detailTotalItems ? (detailPage - 1) * DETAIL_PAGE_SIZE + 1 : 0
  const detailRangeEnd = Math.min(detailPage * DETAIL_PAGE_SIZE, detailTotalItems)
  const activeDailyPage = dailyPages[dailyPage]
  const dailyTotalPages = Math.max(
    1,
    activeDailyPage?.total_pages
      ?? Math.ceil((history.selection.length || 30) / DETAIL_PAGE_SIZE),
  )
  const dailyTotalItems = activeDailyPage?.total_items ?? history.selection.length
  const dailyRangeStart = dailyTotalItems ? (dailyPage - 1) * DETAIL_PAGE_SIZE + 1 : 0
  const dailyRangeEnd = Math.min(dailyPage * DETAIL_PAGE_SIZE, dailyTotalItems)
  const dailyLoading = dailyLoadingPage === dailyPage
  const dailyLastDate = activeDailyPage?.series
    .flatMap((series) => series.points.at(-1)?.[0] ?? [])
    .sort()
    .at(-1)
  const reloadDailyPage = () => {
    setDailyError(null)
    setDailyPages((pages) => {
      const next = { ...pages }
      delete next[dailyPage]
      return next
    })
  }
  const activeViewError = chartMode === 'detail'
    ? detailError
    : chartMode === 'daily'
      ? dailyError
      : null
  const isDelayed = history.status.market_status === 'stale'
    || (history.status.market_status === 'open'
      && !!history.status.last_source_time
      && Date.now() / 1000 - history.status.last_source_time > 10)

  return (
    <main className="min-h-screen bg-slate-50 text-slate-950 dark:bg-slate-950 dark:text-slate-100">
      <header className="sticky top-0 z-30 border-b border-slate-200 bg-white/90 backdrop-blur dark:border-slate-800 dark:bg-slate-950/90">
        <div className="mx-auto flex max-w-[1800px] items-center gap-3 px-4 py-3">
          <div className="flex min-w-0 items-center gap-2">
            <h1 className="truncate text-base font-semibold sm:text-lg">行业板块资金流向</h1>
          </div>
        </div>
      </header>

      <div className="mx-auto max-w-[1800px] space-y-3 px-3 py-3 sm:px-4">
        {(loadError || activeViewError || isDelayed || history.status.last_error || history.status.backfill_error) && (
          <div className="flex items-start gap-2 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300">
            <CircleAlert className="mt-0.5 size-4 shrink-0" />
            <span>{loadError || activeViewError || history.status.last_error || history.status.backfill_error || '数据源更新时间超过10秒，曲线可能暂时停滞。'}</span>
          </div>
        )}

        <div className="grid items-start gap-3 lg:grid-cols-[250px_minmax(0,1fr)]">
          <aside className="grid grid-cols-2 gap-2 sm:grid-cols-4 lg:sticky lg:top-20 lg:grid-cols-1">
            <MetricCard
              title="净流入 / 净流出"
              value={`${inflowCount} / ${outflowCount}`}
              detail="申万二级行业 Top 30"
            />
            <MetricCard
              title="当前最强行业"
              value={leader ? displayName(leader.sector_name) : '--'}
              detail={leader ? formatYi(leader.main_net) : '等待交易数据'}
              positive={leader ? leader.main_net >= 0 : undefined}
            />
            <MetricCard
              title="市场状态"
              value={statusLabel(history.status.market_status)}
              detail={`源时间 ${formatTime(history.status.last_source_time)}`}
            />
            {FLOW_METRICS.map(([key, label]) => {
              const value = aggregates[key] || 0
              return (
                <div key={key} className="rounded-xl border border-slate-200 bg-white p-3 shadow-sm dark:border-slate-800 dark:bg-slate-900">
                  <p className="text-xs text-slate-500">{label}净流入合计</p>
                  <div className="mt-2 flex items-center justify-between">
                    <span className={`font-mono text-base font-semibold ${value >= 0 ? 'text-red-600' : 'text-emerald-600'}`}>
                      {formatYi(value)}
                    </span>
                    {value >= 0 ? <TrendingUp className="size-4 text-red-500" /> : <TrendingDown className="size-4 text-emerald-500" />}
                  </div>
                </div>
              )
            })}
          </aside>

          <section className="min-w-0 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-800 dark:bg-slate-900">
            <div className="flex flex-wrap items-start justify-between gap-3 border-b border-slate-100 px-4 py-3 dark:border-slate-800">
              <div className="inline-flex rounded-lg bg-slate-100 p-1 dark:bg-slate-800" role="group" aria-label="资金曲线视图">
                <button
                  type="button"
                  onClick={() => setChartMode('main')}
                  aria-pressed={chartMode === 'main'}
                  className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                    chartMode === 'main'
                      ? 'bg-white text-slate-900 shadow-sm dark:bg-slate-700 dark:text-white'
                      : 'text-slate-500 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
                  }`}
                >
                  主力资金累计
                </button>
                <button
                  type="button"
                  onClick={() => setChartMode('detail')}
                  aria-pressed={chartMode === 'detail'}
                  className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                    chartMode === 'detail'
                      ? 'bg-white text-slate-900 shadow-sm dark:bg-slate-700 dark:text-white'
                      : 'text-slate-500 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
                  }`}
                >
                  行业细分流向
                </button>
                <button
                  type="button"
                  onClick={() => setChartMode('daily')}
                  aria-pressed={chartMode === 'daily'}
                  className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
                    chartMode === 'daily'
                      ? 'bg-white text-slate-900 shadow-sm dark:bg-slate-700 dark:text-white'
                      : 'text-slate-500 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
                  }`}
                >
                  30日主力流向
                </button>
              </div>
              <div className="min-w-0 text-right">
                <h2 className="font-medium">
                  {chartMode === 'main'
                    ? '主力资金累计曲线'
                    : chartMode === 'detail'
                      ? '行业细分资金流向'
                      : '30日主力资金流向曲线'}
                </h2>
                {chartMode !== 'detail' ? (
                  <div className="mt-1 flex flex-wrap items-center justify-end gap-3 text-xs text-slate-500">
                    <span className="flex items-center gap-1"><span className="size-2 rounded-full bg-red-600" />净流入</span>
                    <span className="flex items-center gap-1"><span className="size-2 rounded-full bg-emerald-600" />净流出</span>
                    <span className="flex items-center gap-1">
                      <Clock3 className="size-3" />
                      {chartMode === 'main'
                        ? formatTime(history.status.last_source_time)
                        : `日频 · 截至 ${dailyLastDate ?? '--'}`}
                    </span>
                  </div>
                ) : (
                  <div className="mt-1 flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-xs text-slate-500">
                    {DETAIL_METRICS.map((metric) => (
                      <span key={metric.key} className="flex items-center gap-1">
                        <span className="size-2 rounded-full" style={{ backgroundColor: metric.color }} />
                        {metric.label}
                      </span>
                    ))}
                    <span className="flex items-center gap-1">
                      <Clock3 className="size-3" />
                      {formatTime(history.status.last_source_time)}
                    </span>
                  </div>
                )}
              </div>
            </div>

            <div className={chartMode === 'main' ? 'relative h-[480px] min-h-[420px] w-full sm:h-[560px] lg:h-[640px]' : 'hidden'}>
                <div ref={chartContainer} className="absolute inset-0" aria-label="行业主力资金实时曲线" />
                {loading && (
                  <div className="absolute inset-0 flex items-center justify-center bg-white/70 text-sm text-slate-500 backdrop-blur-sm dark:bg-slate-900/70">
                    <Radio className="mr-2 size-4 animate-pulse" />正在加载今日资金数据…
                  </div>
                )}
                {!loading && history.series.length === 0 && !loadError && (
                  <div className="absolute inset-0 flex items-center justify-center text-sm text-slate-500">
                    当前还没有今日快照，开盘后将自动开始绘制。
                  </div>
                )}
            </div>

            {chartMode === 'detail' && (
              <div className="flex h-[480px] min-h-[420px] flex-col bg-slate-50 sm:h-[560px] lg:h-[640px] dark:bg-slate-950/40">
                <div className="border-b border-slate-200 px-4 py-2.5 text-xs text-slate-500 dark:border-slate-800">
                  第 {detailRangeStart || '--'}–{detailRangeEnd || '--'} / Top {detailTotalItems || 30} 行业
                </div>

                <div className="min-h-0 flex-1 overflow-y-auto">
                  {detailLoadingPage === detailPage && !activeDetailPage && (
                    <div className="flex h-full items-center justify-center text-sm text-slate-500">
                      <Radio className="mr-2 size-4 animate-pulse" />正在加载行业细分曲线…
                    </div>
                  )}
                  {detailError && !activeDetailPage && (
                    <div className="flex h-full flex-col items-center justify-center gap-3 px-4 text-center text-sm text-slate-500">
                      <span>行业细分曲线加载失败：{detailError}</span>
                      <button
                        type="button"
                        onClick={() => {
                          setDetailError(null)
                          setDetailPages((pages) => ({ ...pages }))
                        }}
                        className="rounded-md border border-slate-300 bg-white px-3 py-1.5 font-medium hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800"
                      >
                        重试
                      </button>
                    </div>
                  )}
                  {activeDetailPage && activeDetailPage.series.length > 0 && (
                    <div className="grid grid-cols-1 gap-3 p-3 xl:h-full xl:grid-cols-3 xl:grid-rows-2">
                      {activeDetailPage.series.map((sector) => (
                        <DetailSectorChart
                          key={sector.sector_code}
                          sector={sector}
                          flashing={flashingEndpoints.has(sector.sector_code)}
                        />
                      ))}
                    </div>
                  )}
                  {activeDetailPage && activeDetailPage.series.length === 0 && (
                    <div className="flex h-full items-center justify-center text-sm text-slate-500">
                      当前还没有行业细分历史数据。
                    </div>
                  )}
                </div>
                <div className="flex items-center justify-center gap-2 border-t border-slate-200 px-4 py-3 text-xs text-slate-500 dark:border-slate-800">
                  <button
                    type="button"
                    onClick={() => setDetailPage((page) => Math.max(1, page - 1))}
                    disabled={detailPage <= 1}
                    className="inline-flex h-8 items-center gap-1 rounded-md border border-slate-300 bg-white px-2.5 font-medium text-slate-700 transition-colors hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
                  >
                    <ChevronLeft className="size-3.5" />上一页
                  </button>
                  <span className="min-w-12 text-center font-mono tabular-nums">{detailPage} / {detailTotalPages}</span>
                  <button
                    type="button"
                    onClick={() => setDetailPage((page) => Math.min(detailTotalPages, page + 1))}
                    disabled={detailPage >= detailTotalPages}
                    className="inline-flex h-8 items-center gap-1 rounded-md border border-slate-300 bg-white px-2.5 font-medium text-slate-700 transition-colors hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
                  >
                    下一页<ChevronRight className="size-3.5" />
                  </button>
                </div>
              </div>
            )}

            {chartMode === 'daily' && (
              <div className="relative flex h-[480px] min-h-[420px] flex-col bg-slate-50 sm:h-[560px] lg:h-[640px] dark:bg-slate-950/40">
                <div className="border-b border-slate-200 px-4 py-2.5 text-xs text-slate-500 dark:border-slate-800">
                  第 {dailyRangeStart || '--'}–{dailyRangeEnd || '--'} / Top {dailyTotalItems || 30} 行业
                </div>

                <div className="relative min-h-0 flex-1 overflow-y-auto">
                  {dailyLoading && !activeDailyPage && (
                    <div className="flex h-full items-center justify-center bg-white/70 text-sm text-slate-500 backdrop-blur-sm dark:bg-slate-900/70">
                    <Radio className="mr-2 size-4 animate-pulse" />正在拉取30日日频资金数据…
                    </div>
                  )}
                  {dailyError && !activeDailyPage && !dailyLoading && (
                    <div className="flex h-full flex-col items-center justify-center gap-3 px-4 text-center text-sm text-slate-500">
                      <span>30日日频资金数据加载失败：{dailyError}</span>
                      <button
                        type="button"
                        onClick={reloadDailyPage}
                        className="rounded-md border border-slate-300 bg-white px-3 py-1.5 font-medium hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-900 dark:hover:bg-slate-800"
                      >
                        重试
                      </button>
                    </div>
                  )}
                  {activeDailyPage && activeDailyPage.series.length > 0 && (
                    <div className="grid grid-cols-1 gap-3 p-3 xl:h-full xl:grid-cols-3 xl:grid-rows-2">
                      {activeDailyPage.series.map((sector) => (
                        <DailySectorChart key={sector.sector_code} sector={sector} />
                      ))}
                    </div>
                  )}
                  {activeDailyPage && activeDailyPage.series.length === 0 && !dailyLoading && !dailyError && (
                    <div className="flex h-full items-center justify-center text-sm text-slate-500">
                      当前还没有30日日频资金数据。
                    </div>
                  )}
                  {activeDailyPage && (
                    activeDailyPage.failed_codes.length > 0
                    || (activeDailyPage.refresh_failed_codes?.length ?? 0) > 0
                  ) && (
                    <div className="absolute left-3 top-3 z-10 flex items-center gap-2 rounded-md bg-amber-50/90 px-2 py-1 text-xs text-amber-700 shadow-sm dark:bg-amber-950/80 dark:text-amber-300">
                      <span>
                        {activeDailyPage.failed_codes.length > 0
                          ? `${activeDailyPage.failed_codes.length} 个行业日频数据暂未返回`
                          : `${activeDailyPage.refresh_failed_codes?.length ?? 0} 个行业更新失败，已显示本地缓存`}
                      </span>
                      <button
                        type="button"
                        onClick={reloadDailyPage}
                        disabled={dailyLoading}
                        className="rounded border border-amber-300/80 bg-white/60 px-1.5 py-0.5 font-medium transition-colors hover:bg-white disabled:cursor-not-allowed disabled:opacity-50 dark:border-amber-700 dark:bg-amber-950/50 dark:hover:bg-amber-900"
                      >
                        {dailyLoading ? '补拉中…' : '补拉'}
                      </button>
                    </div>
                  )}
                </div>
                <div className="flex items-center justify-center gap-2 border-t border-slate-200 px-4 py-3 text-xs text-slate-500 dark:border-slate-800">
                  <button
                    type="button"
                    onClick={() => setDailyPage((page) => Math.max(1, page - 1))}
                    disabled={dailyPage <= 1}
                    className="inline-flex h-8 items-center gap-1 rounded-md border border-slate-300 bg-white px-2.5 font-medium text-slate-700 transition-colors hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
                  >
                    <ChevronLeft className="size-3.5" />上一页
                  </button>
                  <span className="min-w-12 text-center font-mono tabular-nums">{dailyPage} / {dailyTotalPages}</span>
                  <button
                    type="button"
                    onClick={() => setDailyPage((page) => Math.min(dailyTotalPages, page + 1))}
                    disabled={dailyPage >= dailyTotalPages}
                    className="inline-flex h-8 items-center gap-1 rounded-md border border-slate-300 bg-white px-2.5 font-medium text-slate-700 transition-colors hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-40 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-200 dark:hover:bg-slate-800"
                  >
                    下一页<ChevronRight className="size-3.5" />
                  </button>
                </div>
              </div>
            )}
          </section>
        </div>
      </div>
    </main>
  )
}
