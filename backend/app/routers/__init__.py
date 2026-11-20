"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import archive, pipeline, resource

# 固定路径路由（archive、pipeline）必须先于通用 /api/{module}[/{entry_id}]
# 注册，否则 /api/archive/batches、/api/pipeline/dedup-stats 这类路径
# 会被动态路径当成模块名/entry_id 抢先匹配。
ROUTERS = [archive, pipeline, resource]
