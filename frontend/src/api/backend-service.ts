import { fetchJson, request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export type RankingBatchEntry = {
  rank: number
  group_key: string
  [field: string]: string | number
}

export type RankingBatch = {
  batch_id: string
  board_id: string
  metric: {
    metric_id: string
    metric_name: string
    field_name: string
    aggregation: string
  }
  group_fields: string[]
  code_field: string
  top_n: number
  source_digest: string
  digest: string
  entries: RankingBatchEntry[]
  regions: string[]
}

export type RankingPayload = {
  rank_id: string
  scenario: string
  scenario_name: string
  order: string
  batch: RankingBatch
}

export type ReviewRegion = {
  rank: number
  batch_rank: number
  region: string
  region_code: string
  value: number
  source_value: number
  transaction_count: number
  transaction_ids: string[]
  refund: number
  previous_audited: boolean
  selected: boolean
  tie_reason: string
}

export type ReviewReport = {
  batch_id: string
  round_key: string
  metric: RankingBatch['metric']
  dimension: string
  code_field: string
  quota: number
  tie_breaker: string
  previous_regions: string[]
  regions: ReviewRegion[]
  selected: string[]
  target: string | null
  total_refund: number
  regions_in_batch?: string[]
  status?: string
  revision?: number
  digest: string
}

export type ReviewPayload = {
  rank_id: string
  scenario: string
  scenario_name: string
  order: string
  report: ReviewReport
  first_registration: boolean
}

export type ScenarioMeta = {
  key: string
  name: string
  description: string
  quota: number
  previous_regions: string[]
  transaction_count: number
}

export async function listScenarios(): Promise<{ default: string; items: ScenarioMeta[] }> {
  return fetchJson('/api/ranking_board/scenarios')
}

export async function loadRanking(
  entryId: number,
  scenario: string,
  order: string,
): Promise<RankingPayload> {
  const query = new URLSearchParams({ scenario, order }).toString()
  return fetchJson(`/api/ranking_board/${entryId}/ranking?${query}`)
}

export async function submitReview(
  entryId: number,
  scenario: string,
  order: string,
  register = true,
): Promise<ReviewPayload> {
  const response = await request(`/api/ranking_board/${entryId}/review`, {
    method: 'POST',
    body: JSON.stringify({ scenario, order, register }),
  })
  if (!response.ok) throw new Error('指标复核失败')
  return (await response.json()) as ReviewPayload
}

export async function approveReview(entryId: number, roundKey: string): Promise<ReviewReport> {
  const response = await request(
    `/api/ranking_board/${entryId}/reviews/${encodeURIComponent(roundKey)}/approve`,
    { method: 'POST' },
  )
  if (!response.ok) throw new Error('审定抽核失败')
  return (await response.json()) as ReviewReport
}

export async function recomputeReview(
  entryId: number,
  roundKey: string,
  scenario: string,
  order: string,
): Promise<{ recomputed: ReviewReport & { matches: boolean; recomputed_target: string }; report: ReviewReport }> {
  const response = await request(
    `/api/ranking_board/${entryId}/reviews/${encodeURIComponent(roundKey)}/recompute`,
    { method: 'POST', body: JSON.stringify({ scenario, order }) },
  )
  if (!response.ok) throw new Error('复算抽核失败')
  return await response.json()
}

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
