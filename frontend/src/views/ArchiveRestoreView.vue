<template>
  <section class="page" data-testid="archive-restore-panel">
    <header class="page-head">
      <div>
        <h2>归档恢复核对</h2>
        <p class="page-desc">
          把旧版 zlib 归档迁移为带分区摘要的 gzip v2，按分区恢复并核对实际落湖的记录。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="archive-refresh" type="button" @click="reload">刷新批次</button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="runRestore">
      <label class="filter-item">
        <span>归档批次</span>
        <select v-model="batchId" data-testid="archive-batch">
          <option v-for="batch in batches" :key="batch.batch_id" :value="batch.batch_id">
            {{ batch.batch_id }}（{{ batch.partitions.join('、') || '无分区' }}）
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>分区（逗号分隔）</span>
        <input v-model="partitionInput" data-testid="archive-partitions" placeholder="order,inventory" />
      </label>
      <label class="filter-item">
        <span>恢复模式</span>
        <select v-model="mode" data-testid="archive-mode">
          <option value="auto">auto（优先已校验 gzip v2）</option>
          <option value="gzip">gzip（只允许已校验 gzip v2）</option>
          <option value="legacy">legacy（旧版恢复环境）</option>
        </select>
      </label>
      <button class="btn primary" data-testid="archive-restore" type="submit" :disabled="restoring">
        {{ restoring ? '恢复中…' : '恢复分区' }}
      </button>
      <button class="btn" type="button" data-testid="archive-migrate" @click="runMigrate">
        迁移所选分区
      </button>
    </form>

    <p v-if="message" class="page-desc" data-testid="archive-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="archive-error">{{ errorMessage }}</p>

    <table class="data-table" data-testid="archive-receipts">
      <thead>
        <tr>
          <th>分区</th>
          <th>回执</th>
          <th>结果</th>
          <th>实际使用的压缩文件</th>
          <th>来源格式</th>
          <th>已校验</th>
          <th>恢复条数</th>
          <th>去重跳过</th>
          <th>落湖分区</th>
          <th>被拒绝候选</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="receipt in receipts"
          :key="receipt.receipt_id"
          :data-testid="`archive-receipt-${receipt.partition}`"
          :data-status="receipt.status"
        >
          <td>{{ receipt.partition }}</td>
          <td>{{ receipt.receipt_id }}</td>
          <td>{{ receipt.status }}</td>
          <td>{{ receipt.source_file ?? '—' }}</td>
          <td>{{ receipt.source_format ?? '—' }}</td>
          <td>{{ receipt.source_verified ? '是' : '否' }}</td>
          <td>{{ receipt.restored_count }}</td>
          <td>{{ receipt.skipped_count }}</td>
          <td>{{ receipt.lake_partition || '—' }}</td>
          <td>
            <span v-if="!receipt.rejected_candidates.length">—</span>
            <span v-for="candidate in receipt.rejected_candidates" :key="`${candidate.file}-${candidate.reason}`">
              {{ candidate.file }}（{{ candidate.reason }}）；
            </span>
          </td>
        </tr>
        <tr v-if="!receipts.length">
          <td colspan="10" class="empty-state">尚未执行恢复</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">恢复清单</h3>
    <table class="data-table" data-testid="archive-manifest">
      <thead>
        <tr>
          <th>分区</th>
          <th>状态</th>
          <th>来源文件</th>
          <th>来源格式</th>
          <th>已恢复来源记录</th>
          <th>最近回执</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="(entry, partition) in manifestEntries"
          :key="partition"
          :data-testid="`archive-manifest-${partition}`"
          :data-status="entry.status"
        >
          <td>{{ partition }}</td>
          <td>{{ entry.status }}</td>
          <td>{{ entry.source_file ?? '—' }}</td>
          <td>{{ entry.source_format ?? '—' }}</td>
          <td>{{ (entry.event_ids ?? []).join('、') || '—' }}</td>
          <td>{{ entry.last_receipt_id ?? '—' }}</td>
        </tr>
        <tr v-if="!manifestEntries.length">
          <td colspan="6" class="empty-state">该批次还没有恢复记录</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">落湖核对</h3>
    <div class="filter-bar">
      <label v-for="receipt in successfulReceipts" :key="receipt.partition" class="filter-item">
        <span>{{ receipt.partition }} 来源行</span>
        <input :value="receipt.restored_event_ids.join(',')" readonly />
      </label>
      <button class="btn" data-testid="archive-lake-check" type="button" @click="checkLake">
        查询数据湖
      </button>
    </div>
    <table class="data-table" data-testid="archive-lake">
      <thead>
        <tr><th>分区</th><th>湖内事件数</th><th>事件身份</th><th>回执</th></tr>
      </thead>
      <tbody>
        <tr v-for="item in lakeChecks" :key="item.partition" :data-testid="`archive-lake-${item.partition}`">
          <td>{{ item.partition }}</td>
          <td>{{ item.total }}</td>
          <td>{{ item.event_ids.join('、') || '—' }}</td>
          <td>{{ item.receipt_ids.join('、') || '—' }}</td>
        </tr>
        <tr v-if="!lakeChecks.length">
          <td colspan="4" class="empty-state">点击“查询数据湖”核对实际恢复的记录</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  getRestoreManifest,
  listArchiveBatches,
  migratePartitions,
  queryLake,
  restorePartitions,
} from '@/api/archive-service'
import type {
  ArchiveBatch,
  RestoreManifest,
  RestoreReceipt,
} from '@/api/archive-service'

const batches = ref<ArchiveBatch[]>([])
const batchId = ref('')
const partitionInput = ref('order,inventory')
const mode = ref('auto')
const receipts = ref<RestoreReceipt[]>([])
const manifest = ref<RestoreManifest | null>(null)
const lakeChecks = ref<{ partition: string; total: number; event_ids: string[]; receipt_ids: string[] }[]>([])
const restoring = ref(false)
const message = ref('')
const errorMessage = ref('')

const manifestEntries = computed(() =>
  Object.values(manifest.value?.partitions ?? {}),
)
const successfulReceipts = computed(() =>
  receipts.value.filter((receipt) => receipt.status !== 'failed'),
)

function partitions(): string[] {
  return partitionInput.value
    .split(/[，,]/)
    .map((item) => item.trim())
    .filter(Boolean)
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listArchiveBatches()
    batches.value = payload.batches
    if (!batchId.value && batches.value.length) {
      batchId.value = batches.value[0].batch_id
    }
    if (batchId.value) {
      manifest.value = (await getRestoreManifest(batchId.value)).manifest
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归档批次读取失败'
  }
}

async function runRestore() {
  restoring.value = true
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await restorePartitions(batchId.value, partitions(), mode.value)
    receipts.value = result.receipts
    manifest.value = (await getRestoreManifest(batchId.value)).manifest
    message.value = `恢复运行 ${result.run_id}：${
      result.receipts.map((r) => `${r.partition}=${r.status}`).join('，')
    }`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复失败'
  } finally {
    restoring.value = false
  }
}

async function runMigrate() {
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await migratePartitions(batchId.value, partitions())
    message.value = `迁移结果：${
      result.results.map((r) => `${r.partition}=${r.status}(${r.reason})`).join('，')
    }`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迁移失败'
  }
}

async function checkLake() {
  errorMessage.value = ''
  try {
    const checks = []
    for (const receipt of successfulReceipts.value) {
      const lake = await queryLake(receipt.partition)
      checks.push({
        partition: receipt.partition,
        total: lake.total,
        event_ids: lake.event_ids,
        receipt_ids: lake.receipt_ids,
      })
    }
    lakeChecks.value = checks
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '落湖核对失败'
  }
}

onMounted(reload)
</script>
