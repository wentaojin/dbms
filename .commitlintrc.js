module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'type-enum': [
      2,
      'always',
      [
        'feats',
        'feat',     // 新功能
        'fix',      // Bug 修复
        'fixs'
        'docs',     // 文档
        'chore',    // 构建/辅助工具
        'style',    // 样式修改（不影响代码行为）
        'refactor', // 重构
        'perf',     // 性能优化
        'test',     // 测试
        'build',    // 构建系统
        'ci',       // CI/CD
        'revert'    // 回滚
      ]
    ],
    'subject-case': [0], // 不强制 subject 大小写格式
  },
};
