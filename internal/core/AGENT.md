# Context

核心业务逻辑与资源层：负责执行基于 expr-lang 的表达式并验证。管理榜单数据的数据结构和抽象 Redis Repository（使用 pipeline 批量更新）确保性能和高可用。
