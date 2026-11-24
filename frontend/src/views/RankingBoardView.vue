<template>
  <section class="page" data-module="ranking_board">
    <header class="page-head">
      <div>
        <h2>排行榜 · 分组排行与指标复核</h2>
        <p class="page-desc">
          排行先按指标口径把交易聚合到地区，再对分组取名次；指标复核只读取本批次真正入榜的地区。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/ranking_board/list">方案清单</RouterLink>
      </div>
    </header>

    <form class="analysis-form" @submit.prevent="refreshAll">
      <label>
        <span>排行方案</span>
        <select v-model.number="entryId" data-testid="entry-select">
          <option v-for="item in schemes" :key="String(item.id)" :value="Number(item.id)">
            {{ item['方案编号'] }} · {{ item['排行指标'] }}
          </option>
        </select>
      </label>
      <label>
        <span>复核场景</span>
        <select v-model="scenario" data-testid="scenario-select">
          <option v-for="item in scenarios" :key="item.key" :value="item.key">
            {{ item.name }}
          </option>
        </select>
      </label>
      <label>
        <span>交易输入顺序</span>
        <select v-model="order" data-testid="order-select">
          <option value="forward">正向</option>
          <option value="reverse">反向</option>
        </select>
      </label>
      <button class="btn primary" data-testid="compute" type="submit" :disabled="loading">
        {{ loading ? '计算中…' : '打开实际排行与抽核报告' }}
      </button>
      <button class="btn" data-testid="review-submit" type="button" :disabled="loading" @click="submit">
        重复提交同一次抽核
      </button>
      <button class="btn" data-testid="approve" type="button" :disabled="loading || !report" @click="approve">
        审定抽核
      </button>
      <button class="btn" data-testid="recompute" type="button" :disabled="loading || !report" @click="recompute">
        复算已审定抽核
      </button>
    </form>

    <p v-if="scenarioMeta" class="page-desc" data-testid="scenario-desc">
      {{ scenarioMeta.description }}
    </p>
    <p v-if="message" class="status-legend" data-testid="status-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="error-message">{{ errorMessage }}</p>

    <template v-if="batch">
      <h3>分组排行批次</h3>
      <p class="page-desc">
        批次 {{ batch.batch_id }} · 指标 {{ batch.metric.metric_name }}（{{ batch.metric.aggregation }}）
        · 分组 {{ batch.group_fields.join(' / ') }} · 输入顺序：{{ order === 'forward' ? '正向' : '反向' }}
      </p>
      <table class="data-table" data-testid="ranking-table">
        <thead>
          <tr>
            <th>名次</th>
            <th>地区</th>
            <th>地区编号</th>
            <th>{{ batch.metric.field_name }}</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in batch.entries"
            :key="String(item.group_key)"
            :data-testid="`ranking-row-${item.rank}`"
          >
            <td>{{ item.rank }}</td>
            <td>{{ item.group_key }}</td>
            <td>{{ item[batch.code_field] ?? '—' }}</td>
            <td>{{ item[batch.metric.field_name] }}</td>
          </tr>
        </tbody>
      </table>
    </template>

    <template v-if="report">
      <h3>指标复核 / 抽核报告</h3>
      <p class="page-desc">
        轮次 {{ report.round_key }} · 名额 {{ report.quota }} ·
        本轮实际交给 <strong data-testid="review-target">{{ report.target ?? '—' }}</strong> ·
        同额规则：{{ report.tie_breaker }} · 状态 {{ report.status ?? '计算' }}
      </p>
      <table class="data-table" data-testid="review-table">
        <thead>
          <tr>
            <th>名次</th>
            <th>地区</th>
            <th>地区编号</th>
            <th>指标值</th>
            <th>成交笔数</th>
            <th>退款</th>
            <th>上期抽核</th>
            <th>本轮抽核</th>
            <th>同额定序</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in report.regions"
            :key="item.region"
            :data-testid="`review-row-${item.region}`"
          >
            <td>{{ item.rank }}</td>
            <td>{{ item.region }}</td>
            <td>{{ item.region_code || '—' }}</td>
            <td :data-testid="`review-value-${item.region}`">{{ item.value }}</td>
            <td :data-testid="`review-count-${item.region}`">{{ item.transaction_count }}</td>
            <td :data-testid="`review-refund-${item.region}`">{{ item.refund }}</td>
            <td>{{ item.previous_audited ? '是' : '否' }}</td>
            <td :data-testid="`review-selected-${item.region}`">{{ item.selected ? '是' : '否' }}</td>
            <td>{{ item.tie_reason || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  approveReview,
  listEntries,
  loadRanking,
  listScenarios,
  recomputeReview,
  submitReview,
} from '@/api/backend-service'
import type { RankingBatch, ReviewReport, ScenarioMeta } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const schemes = ref<EntryRow[]>([])
const scenarios = ref<ScenarioMeta[]>([])
const entryId = ref(1)
const scenario = ref('west_north')
const order = ref<'forward' | 'reverse'>('forward')
const batch = ref<RankingBatch | null>(null)
const report = ref<ReviewReport | null>(null)
const loading = ref(false)
const message = ref('')
const errorMessage = ref('')

const scenarioMeta = computed(() => scenarios.value.find((item) => item.key === scenario.value))

async function refreshAll() {
  await loadBatchAndReview(true)
}

async function loadBatchAndReview(register: boolean) {
  loading.value = true
  errorMessage.value = ''
  try {
    const [ranking, review] = await Promise.all([
      loadRanking(entryId.value, scenario.value, order.value),
      submitReview(entryId.value, scenario.value, order.value, register),
    ])
    batch.value = ranking.batch
    report.value = review.report
    message.value = register
      ? review.first_registration
        ? `抽核已登记：本轮交给 ${review.report.target ?? '—'}`
        : `同一次抽核此前已登记，本次未重复登记，仍为 ${review.report.target ?? '—'}`
      : `批次已重算：本轮会交给 ${review.report.target ?? '—'}`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '排行/复核读取失败'
  } finally {
    loading.value = false
  }
}

async function submit() {
  await loadBatchAndReview(true)
}

async function approve() {
  if (!report.value) return
  loading.value = true
  errorMessage.value = ''
  try {
    report.value = await approveReview(entryId.value, report.value.round_key)
    message.value = `抽核轮次 ${report.value.round_key} 已审定`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '审定失败'
  } finally {
    loading.value = false
  }
}

async function recompute() {
  if (!report.value) return
  loading.value = true
  errorMessage.value = ''
  try {
    const result = await recomputeReview(
      entryId.value,
      report.value.round_key,
      scenario.value,
      order.value,
    )
    message.value = result.recomputed.matches
      ? `复算一致：已审定抽核仍为 ${result.recomputed.recomputed_target}`
      : `复算结果发生漂移：${result.recomputed.recomputed_target}`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '复算失败'
  } finally {
    loading.value = false
  }
}

onMounted(async () => {
  try {
    const [page, scenarioPayload] = await Promise.all([
      listEntries('ranking_board'),
      listScenarios(),
    ])
    schemes.value = page.items
    if (page.items.length && !page.items.some((row) => Number(row.id) === entryId.value)) {
      entryId.value = Number(page.items[0].id)
    }
    scenarios.value = scenarioPayload.items
    scenario.value = scenarioPayload.default
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '初始化失败'
  }
  await loadBatchAndReview(true)
})
</script>
