"""账号级标注支撑：证据包构建、LLM 预标注、金标准与一致性评估。

设计原则（见 docs/annotation_guidelines.md）：

- 标注单位是账号（``username@home_host``），不是单条帖子；
- LLM 只看到「已结构化的证据包」，不直接读原始时间线；
- 输出必须引用可核验证据（统计量 / note_key），无证据即 ``uncertain``；
- ``accounts.is_bot_declared`` 仅作辅助，不能单独构成判据；
- 人类专家负责规范、金标准、仲裁与审计，模型只负责预标注。
"""

from __future__ import annotations

EVIDENCE_VERSION = "evidence_v1"
PROMPT_VERSION = "label_v1"
LABELS = ("bot", "human", "uncertain")
CONFIDENCES = ("high", "medium", "low")

__all__ = ["EVIDENCE_VERSION", "PROMPT_VERSION", "LABELS", "CONFIDENCES"]
