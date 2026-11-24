import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import RankingBoardView from '@/views/RankingBoardView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 排行榜的默认入口是分组排行 + 复核工作台，清单仍保留在 /ranking_board/list。
    { path: '/ranking_board', name: 'ranking-board', component: RankingBoardView },
    { path: '/ranking_board/list', name: 'ranking-board-list', component: WorkbenchView, props: { moduleKey: 'ranking_board' } },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
