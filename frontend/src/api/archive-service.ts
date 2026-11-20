import { request } from './client'

/** 归档迁移/恢复接口：与后端 /api/archive 一一对应。 */

export type ArchiveBatch = {
  batch_id: string
  archive_date: string
  file_count: number
  partitions: string[]
  formats: string[]
}

export type ArchiveAsset = {
  asset_id: string
  batch_id: string
  partition: string
  archive_date: string
  format: string
  file: string
  version: number
  status: string
  record_count: number
  event_ids: string[]
  partition_digest: string
  compressed_sha256: string
}

export type RejectedCandidate = {
  file: string
  format?: string | null
  reason: string
  detail?: string
  usable: boolean
}

export type RestoreReceipt = {
  receipt_id: string
  batch_id: string
  run_id: string
  partition: string
  mode: string
  status: string
  source_file: string | null
  source_files: string[]
  source_format: string | null
  source_verified: boolean
  source_version: number
  digest: string
  archive_date: string
  restored_count: number
  skipped_count: number
  restored_event_ids: string[]
  skipped_event_ids: string[]
  lake_partition: string
  lake_dates: string[]
  lake_files: string[]
  rejected_candidates: RejectedCandidate[]
  reason: string
  message?: string
}

export type RestoreResult = {
  ok: boolean
  batch_id: string
  run_id: string
  mode: string
  receipts: RestoreReceipt[]
  partitions: Record<string, RestoreReceipt>
  manifest_path: string
}

export type ManifestPartition = {
  partition: string
  status: string
  source_file?: string
  source_format?: string
  source_verified?: boolean
  restored_count?: number
  event_ids?: string[]
  last_receipt_id?: string
  last_failure?: { reason?: string; message?: string }
}

export type RestoreManifest = {
  batch_id: string
  updated_at: string
  partitions: Record<string, ManifestPartition>
  restore_runs: { run_id: string; status: string; requested_partitions: string[] }[]
}

async function json<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    const message =
      typeof detail?.detail === 'string' ? detail.detail : detail?.detail?.message
    throw new Error(message ?? `归档接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

export async function listArchiveBatches(): Promise<{ ok: boolean; batches: ArchiveBatch[] }> {
  return json(await request('/api/archive/batches'))
}

export async function migratePartitions(
  batchId: string,
  partitions: string[],
): Promise<{ ok: boolean; results: { partition: string; status: string; reason: string }[] }> {
  return json(await request(`/api/archive/${encodeURIComponent(batchId)}/migrate`, {
    method: 'POST',
    body: JSON.stringify({ partitions }),
  }))
}

export async function restorePartitions(
  batchId: string,
  partitions: string[],
  mode = 'auto',
  requester = 'workbench',
): Promise<RestoreResult> {
  return json(await request('/api/archive/restore', {
    method: 'POST',
    body: JSON.stringify({ batch_id: batchId, partitions, mode, requester }),
  }))
}

export async function getRestoreManifest(
  batchId: string,
): Promise<{ ok: boolean; manifest: RestoreManifest }> {
  return json(await request(`/api/archive/${encodeURIComponent(batchId)}/manifest`))
}

export async function queryLake(
  partition: string,
  date?: string,
): Promise<{
  ok: boolean
  partition: string
  total: number
  event_ids: string[]
  receipt_ids: string[]
  records: Record<string, unknown>[]
}> {
  const query = new URLSearchParams({ partition })
  if (date) query.set('date', date)
  return json(await request(`/api/archive/lake?${query.toString()}`))
}

export async function getSourceRowReceipt(
  partition: string,
  eventId: string,
): Promise<{ ok: boolean; receipt: RestoreReceipt }> {
  const query = new URLSearchParams({ partition, event_id: eventId })
  return json(await request(`/api/archive/source-row?${query.toString()}`))
}

export async function listReceipts(
  batchId?: string,
): Promise<{ ok: boolean; items: RestoreReceipt[]; total: number }> {
  const query = new URLSearchParams({ limit: '50' })
  if (batchId) query.set('batch_id', batchId)
  return json(await request(`/api/archive/receipts?${query.toString()}`))
}
