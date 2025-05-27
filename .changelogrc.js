module.exports = {
  config: {
    "types": [
      { type: "feat", section: "🚀 Features" },
      { type: "fix", section: "🐛 Bug Fixes" },
      { type: "docs", section: "📦 Docs" },
      { type: "chore", section: "💬 Chore" },
      { type: "perf", section: "🔥 Performance" },
      { type: "refactor", section: "🛠 Refactor" },
      { type: "test", section: "🧪 Tests" },
      { type: "build", section: "🏗 Build System" },
      { type: "ci", section: "⚙️ CI/CD" },
      { type: "revert", section: "⏪ Reverts" }
    ],
    "labels": {
      "Breaking change": "💥 Breaking Changes",
      "Issue": "🪲 Related Issues"
    }
  },
  releaseCount: 1,
  outputUnreleased: false,
  commitGroupsSort: ["feat", "fix", "perf", "refactor", "test", "docs", "chore", "build", "ci", "revert"],
  noteGroupsSort: ["Breaking change", "Issue"]
};
