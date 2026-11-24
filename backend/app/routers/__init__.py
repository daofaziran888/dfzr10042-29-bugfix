"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

排行榜专项计算路由必须排在通用资源路由之前，避免 /api/ranking_board/...
被 /api/{module}/{entry_id} 抢先匹配。
"""
from __future__ import annotations

from app.routers import ranking, resource

ROUTERS = [ranking, resource]
