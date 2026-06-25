import asyncio
import json
import time
import re
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Set

from core.plugin import BasePlugin, logger, on, Priority, register_tool
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.chat import MessageChain
from core.chat.message_elements import Text, Image, At, File, Record, Video
from core.provider import LLMRequest
from core.agent.message import OpenAIMessage

from .utils.db import DatabaseManager
from .utils.render import HTMLRenderer
from .utils.avatar import AvatarCache


# 预定义暖色单色（用于活跃用户名称）
WARM_COLORS = [
    "#C0392B",  # 红
    "#D35400",  # 橙
    "#E67E22",  # 橙黄
    "#F39C12",  # 黄
    "#8E44AD",  # 紫
    "#2980B9",  # 蓝
    "#1ABC9C",  # 青
    "#27AE60",  # 绿
    "#6B4A3A",  # 棕
    "#E74C3C",  # 亮红
    "#F1C40F",  # 金黄
    "#2C3E50",  # 深蓝
]


class KiraDailyReport(BasePlugin):
    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)

        # ---- 配置加载 ----
        self.enabled_groups = cfg.get("enabled_groups", [])
        self.auto_enabled_groups = cfg.get("auto_enabled_groups", [])
        self.enable_command_trigger = cfg.get("enable_command_trigger", True)
        self.enable_natural_language_trigger = cfg.get("enable_natural_language_trigger", True)
        self.command_prefixes = cfg.get("command_prefixes", ["/日报", "/群日报"])
        self.analysis_days = cfg.get("analysis_days", 1)
        self.max_messages_per_analysis = cfg.get("max_messages_per_analysis", 200)
        self.min_messages_threshold = cfg.get("min_messages_threshold", 10)
        self.auto_analysis_time = cfg.get("auto_analysis_time", "23:59")
        self.enable_auto_analysis = cfg.get("enable_auto_analysis", True)
        self.cooldown_hours = cfg.get("cooldown_hours", 4)
        self.whitelist_users = cfg.get("whitelist_users", [])
        self.whitelist_exempt_cooldown = cfg.get("whitelist_exempt_cooldown", True)
        self.enable_sharp_comment = cfg.get("enable_sharp_comment", True)
        self.inject_persona = cfg.get("inject_persona", True)
        self.persona_id = cfg.get("persona_id", "default")
        self.topic_min = cfg.get("topic_min", 1)
        self.topic_max = cfg.get("topic_max", 5)
        self.quote_max = cfg.get("quote_max", 3)
        self.active_users_max = cfg.get("active_users_max", 5)
        self.max_concurrent_analysis = cfg.get("max_concurrent_analysis", 3)
        self.report_retention_days = cfg.get("report_retention_days", 7)
        self.report_cleanup_count = cfg.get("report_cleanup_count", 7)
        self.output_format = cfg.get("output_format", "image")
        self.verbose_log = cfg.get("verbose_log", False)
        self.avatar_cache_expire_days = cfg.get("avatar_cache_expire_days", 3)
        self.bot_nickname_override = cfg.get("bot_nickname_override", "")
        self.llm_model = cfg.get("llm_model", "")
        self.exclude_senders = cfg.get("exclude_senders", ["system"])
        self.prefer_system_browser = cfg.get("prefer_system_browser", True)
        self.render_timeout = cfg.get("render_timeout", 30)

        # 增量模式配置
        self.enable_incremental_mode = cfg.get("enable_incremental_mode", False)
        self.incremental_group_list = cfg.get("incremental_group_list", [])
        self.incremental_interval_minutes = cfg.get("incremental_interval_minutes", 120)
        self.incremental_min_messages = cfg.get("incremental_min_messages", 10)
        self.window_hours = cfg.get("window_hours", 24)

        # 自定义消息
        self.msg_too_few = cfg.get("msg_too_few", "📊 群聊日报：今日消息数 ({count}) 不足 {threshold} 条，暂不生成日报~")
        self.msg_cooldown = cfg.get("msg_cooldown", "⏳ 日报生成冷却中，剩余 {remaining:.1f} 小时~")
        self.msg_not_enabled = cfg.get("msg_not_enabled", "❌ 该群未启用日报功能")
        self.msg_not_group = cfg.get("msg_not_group", "❌ 请在群聊中使用此功能")
        self.msg_processing = cfg.get("msg_processing", "📊 正在生成日报，请稍候...")
        self.msg_failed = cfg.get("msg_failed", "❌ 日报生成失败：{error}")
        self.msg_natural_disabled = cfg.get("msg_natural_disabled", "ℹ️ 自然语言触发日报已禁用，请使用命令触发（如 /日报）")

        # 存储路径
        self.db_filename = cfg.get("db_path", "messages.db")
        self.reports_dirname = cfg.get("reports_dir", "reports")
        self.avatars_dirname = cfg.get("avatar_cache_dir", "avatars")

        # ---- 初始化 ----
        self.data_dir: Path = ctx.get_plugin_data_dir()
        self.db_path = self.data_dir / self.db_filename
        self.reports_dir = self.data_dir / self.reports_dirname
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.db = DatabaseManager(str(self.db_path))
        self.renderer = HTMLRenderer(
            self.data_dir,
            prefer_system_browser=self.prefer_system_browser,
            render_timeout=self.render_timeout
        )
        self.avatar_cache = AvatarCache(
            self.data_dir / self.avatars_dirname,
            expire_days=self.avatar_cache_expire_days
        )

        self._semaphore = asyncio.Semaphore(self.max_concurrent_analysis)
        self._cooldown_map: Dict[str, float] = {}
        self._scheduler_task: Optional[asyncio.Task] = None
        self._incremental_scheduler_task: Optional[asyncio.Task] = None
        self._running = True

        # 缓存 bot 信息
        self._bot_self_id: Optional[str] = None
        self._bot_nickname: Optional[str] = None
        self._bot_avatar: Optional[str] = None

        logger.info("[KiraDaily] 插件已加载")

    # ============================================================
    # 生命周期
    # ============================================================

    async def initialize(self):
        await self.db.init()
        await self.db.create_tables()
        await self.avatar_cache.init()
        await self._fetch_bot_info()

        if not self.enable_natural_language_trigger:
            try:
                self.ctx.llm_api.unregister_tool("generate_daily_report")
                self._log("自然语言触发已禁用，已从LLM工具列表中移除 generate_daily_report")
            except Exception as e:
                self._log(f"注销工具失败（可能未注册）: {e}")
        else:
            self._log("自然语言触发已启用")

        await self._cleanup_old_reports()

        if self.enable_auto_analysis:
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())
            logger.info(f"[KiraDaily] 定时任务已启动，每天 {self.auto_analysis_time} 执行")

        if self.enable_incremental_mode:
            self._incremental_scheduler_task = asyncio.create_task(self._incremental_scheduler_loop())
            logger.info(f"[KiraDaily] 增量分析已启动，间隔 {self.incremental_interval_minutes} 分钟")

        logger.info("[KiraDaily] 初始化完成")

    async def terminate(self):
        self._running = False
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        if self._incremental_scheduler_task and not self._incremental_scheduler_task.done():
            self._incremental_scheduler_task.cancel()
            try:
                await self._incremental_scheduler_task
            except asyncio.CancelledError:
                pass
        await self.db.close()
        logger.info("[KiraDaily] 已卸载")

    def _log(self, msg: str):
        if self.verbose_log:
            logger.info(f"[KiraDaily] {msg}")

    # ============================================================
    # 获取 Bot 信息（加强版）
    # ============================================================

    async def _fetch_bot_info(self):
        try:
            adapter = self.ctx.adapter_mgr.get_adapter("qq")
            self._bot_self_id = None
            self._bot_nickname = None
            self._bot_avatar = None

            if adapter:
                self._bot_self_id = str(adapter.info.config.get("self_id", ""))
                if not self._bot_self_id:
                    if hasattr(adapter, "bot") and hasattr(adapter.bot, "uin"):
                        self._bot_self_id = str(adapter.bot.uin)
                    elif hasattr(adapter, "bot") and hasattr(adapter.bot, "self_id"):
                        self._bot_self_id = str(adapter.bot.self_id)

                if self._bot_self_id:
                    self._bot_avatar = await self.avatar_cache.get_bot_avatar(self._bot_self_id)
                    if self.bot_nickname_override:
                        self._bot_nickname = self.bot_nickname_override
                    else:
                        if hasattr(adapter, "get_login_info"):
                            try:
                                info = await adapter.get_login_info()
                                if info and info.get("nickname"):
                                    self._bot_nickname = info["nickname"]
                            except Exception:
                                pass
                        if not self._bot_nickname and hasattr(adapter, "bot") and hasattr(adapter.bot, "send_action"):
                            try:
                                result = await adapter.bot.send_action("get_login_info", {})
                                if result and result.get("status") == "ok":
                                    data = result.get("data", {})
                                    if data.get("nickname"):
                                        self._bot_nickname = data["nickname"]
                            except Exception:
                                pass
                        if not self._bot_nickname:
                            self._bot_nickname = "AI"
                else:
                    self._bot_self_id = self.ctx.config.get("bot_config.bot.self_id", "")
                    if self._bot_self_id:
                        self._bot_avatar = await self.avatar_cache.get_bot_avatar(self._bot_self_id)
                        self._bot_nickname = self.bot_nickname_override or "AI"
                    else:
                        self._bot_self_id = "0"
                        self._bot_nickname = self.bot_nickname_override or "AI"
                        self._bot_avatar = await self.avatar_cache.get_bot_avatar("0")
            else:
                self._bot_self_id = self.ctx.config.get("bot_config.bot.self_id", "")
                if self._bot_self_id:
                    self._bot_avatar = await self.avatar_cache.get_bot_avatar(self._bot_self_id)
                    self._bot_nickname = self.bot_nickname_override or "AI"
                else:
                    self._bot_self_id = "0"
                    self._bot_nickname = self.bot_nickname_override or "AI"
                    self._bot_avatar = await self.avatar_cache.get_bot_avatar("0")
        except Exception as e:
            self._log(f"获取 bot 信息失败: {e}")
            self._bot_self_id = "0"
            self._bot_nickname = self.bot_nickname_override or "AI"
            self._bot_avatar = await self.avatar_cache.get_bot_avatar("0")

    # ============================================================
    # 获取群名称（使用 bot.send_action）
    # ============================================================

    async def _get_group_name(self, group_id: str) -> str:
        try:
            adapter = self.ctx.adapter_mgr.get_adapter("qq")
            if not adapter:
                return group_id
            parts = group_id.split(":")
            if len(parts) >= 3:
                qq_group_id = parts[2]
            else:
                qq_group_id = group_id
            if hasattr(adapter, "bot") and hasattr(adapter.bot, "send_action"):
                result = await adapter.bot.send_action("get_group_info", {"group_id": int(qq_group_id)})
                if result and result.get("status") == "ok":
                    data = result.get("data", {})
                    return data.get("group_name", group_id)
            return group_id
        except Exception as e:
            self._log(f"获取群名称失败: {e}")
            return group_id

    # ============================================================
    # 群组权限检查
    # ============================================================

    def _is_group_enabled(self, group_id: str) -> bool:
        if not self.enabled_groups:
            return True
        return group_id in self.enabled_groups

    def _is_auto_enabled(self, group_id: str) -> bool:
        if not self.auto_enabled_groups:
            return self._is_group_enabled(group_id)
        return group_id in self.auto_enabled_groups

    def _is_incremental_group(self, group_id: str) -> bool:
        if not self.enable_incremental_mode:
            return False
        if not self.incremental_group_list:
            return True
        return group_id in self.incremental_group_list

    def _get_group_id_from_event(self, event) -> Optional[str]:
        try:
            if hasattr(event, "sid"):
                sid = event.sid
                if sid.startswith("qq:gm:"):
                    return sid
                return None
            if hasattr(event, "message") and hasattr(event.message, "group"):
                if event.message.group:
                    return f"qq:gm:{event.message.group.group_id}"
        except Exception:
            pass
        return None

    def _get_user_id_from_event(self, event) -> str:
        try:
            if hasattr(event, "message") and hasattr(event.message, "sender"):
                return str(event.message.sender.user_id)
            elif hasattr(event, "messages") and event.messages:
                return str(event.messages[0].sender.user_id)
        except Exception:
            pass
        return "unknown"

    def _get_sid(self, event) -> str:
        if hasattr(event, "sid"):
            return event.sid
        if hasattr(event, "session") and hasattr(event.session, "sid"):
            return event.session.sid
        return "default"

    # ============================================================
    # 直接发送消息到群
    # ============================================================

    async def _send_text_to_group(self, group_id: str, text: str):
        try:
            adapter = self.ctx.adapter_mgr.get_adapter("qq")
            if not adapter:
                return
            parts = group_id.split(":")
            if len(parts) >= 3:
                qq_group_id = parts[2]
            else:
                qq_group_id = group_id
            await adapter.send_group_message(qq_group_id, MessageChain([Text(text)]))
        except Exception as e:
            self._log(f"发送文本到群 {group_id} 失败: {e}")

    # ============================================================
    # 冷却检查
    # ============================================================

    def _is_in_cooldown(self, group_id: str, user_id: str) -> bool:
        if self.cooldown_hours <= 0:
            return False
        if self.whitelist_exempt_cooldown and user_id in self.whitelist_users:
            return False
        last_run = self._cooldown_map.get(group_id, 0)
        if last_run == 0:
            return False
        elapsed = time.time() - last_run
        return elapsed < self.cooldown_hours * 3600

    def _update_cooldown(self, group_id: str):
        self._cooldown_map[group_id] = time.time()

    # ============================================================
    # 消息收集（含屏蔽发送者）
    # ============================================================

    @on.im_message(priority=Priority.HIGH)
    async def collect_message(self, event: KiraMessageEvent):
        if not event.is_group_message():
            return

        group_id = self._get_group_id_from_event(event)
        if not group_id or not self._is_group_enabled(group_id):
            return

        # 检查发送者是否在屏蔽列表中
        sender_nickname = event.message.sender.nickname if event.message.sender else ""
        if sender_nickname in self.exclude_senders:
            self._log(f"屏蔽消息: 发送者 {sender_nickname} 在排除列表中，已忽略")
            return

        text_parts = []
        for elem in event.message.chain:
            if isinstance(elem, Text):
                text_parts.append(elem.text)
            elif isinstance(elem, At):
                text_parts.append(f"@{elem.nickname or elem.pid}")
            elif isinstance(elem, Image):
                text_parts.append("[图片]")
            elif isinstance(elem, (File, Record, Video)):
                text_parts.append(f"[{elem.__class__.__name__}]")

        content = " ".join(text_parts).strip()
        if not content:
            content = "[图片/表情]"

        user_id = str(event.message.sender.user_id)
        nickname = event.message.sender.nickname or "未知"
        timestamp = event.message.timestamp or int(time.time())

        await self.db.save_message(
            session_id=group_id,
            user_id=user_id,
            nickname=nickname,
            content=content,
            timestamp=timestamp
        )
        self._log(f"收集消息: {group_id} | {nickname}: {content[:30]}...")

    # ============================================================
    # LLM 调用（支持自定义模型）
    # ============================================================

    async def _call_llm(self, prompt: str) -> dict:
        if self.llm_model:
            client = self.ctx.get_llm_client(model_uuid=self.llm_model)
            if client is None:
                logger.warning(f"指定的 LLM 模型 {self.llm_model} 不存在，回退到快速模型")
                client = self.ctx.get_default_fast_llm_client()
        else:
            client = self.ctx.get_default_fast_llm_client()

        if client is None:
            raise RuntimeError("没有可用的 LLM 客户端")

        req = LLMRequest(messages=[OpenAIMessage(role="user", content=prompt)])
        resp = await client.chat(req)

        text = resp.text_response.strip()
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {}

    # ============================================================
    # 传统模式：一次性分析
    # ============================================================

    async def _build_analysis_prompt(self, messages: List[dict], truncated: bool) -> str:
        msg_lines = []
        for msg in messages[-200:]:
            ts = datetime.fromtimestamp(msg["timestamp"]).strftime("%H:%M")
            msg_lines.append(f"[{ts}] {msg['nickname']}: {msg['content']}")
        msg_text = "\n".join(msg_lines)

        persona_prompt = ""
        if self.inject_persona:
            try:
                if self.persona_id and self.persona_id != "default":
                    persona = await self.ctx.persona_mgr.get_persona(self.persona_id)
                else:
                    personas = await self.ctx.persona_mgr.list_personas()
                    persona = personas[0] if personas else None
                if persona:
                    persona_prompt = f"\n请以以下人设风格进行分析和点评：\n{persona.content}\n"
            except Exception as e:
                self._log(f"获取人设失败: {e}")

        sharp_instruction = """
5. 锐评：用一句话对今天的群聊进行锐评（毒舌、幽默或温柔），语气符合人设风格。
""" if self.enable_sharp_comment else ""

        prompt = f"""你是一个群聊数据分析师。请分析以下群聊记录，生成一份简洁的日报。

{persona_prompt}

【分析要求】
1. 一句话总结：用一句简短有趣的话概括今天群聊的氛围（放在日报头部）。
2. 统计数据：总消息数、参与人数、最活跃时段（请给出时间段，格式如 "20:00-22:00"）。
3. 热门话题：提取{self.topic_min}-{self.topic_max}个主要话题，每个话题包含：标题、描述、AI吐槽（一句话毒舌吐槽）。
4. 活跃用户：列出发言最多的前{self.active_users_max}位用户及发言数。
5. 金句：选出{self.quote_max}条最精彩的发言。如果有多条，标注其中一条为"最佳金句"并说明理由。
{sharp_instruction}

【输出格式】（严格按JSON格式输出）
{{
  "header_comment": "一句话概括今日群聊",
  "stats": {{"total_messages": 0, "participants": 0, "peak_hour": "20:00-22:00"}},
  "topics": [
    {{"title": "话题1", "detail": "简短描述", "roast": "AI吐槽"}}
  ],
  "active_users": [
    {{"name": "用户A", "count": 10}}
  ],
  "quotes": [
    {{"text": "金句内容", "sender": "用户A"}}
  ],
  "best_quote": {{
    "text": "最佳金句内容",
    "sender": "用户A",
    "reason": "AI选取此句的理由"
  }},
  "sharp_comment": "AI锐评一句话"
}}

【聊天记录】
{msg_text}
"""
        return prompt

    # ============================================================
    # 增量模式：精简 Prompt
    # ============================================================

    async def _call_llm_incremental(self, messages: List[dict]) -> dict:
        """增量分析专用（精简版）"""
        msg_lines = []
        for msg in messages[-200:]:
            ts = datetime.fromtimestamp(msg["timestamp"]).strftime("%H:%M")
            msg_lines.append(f"[{ts}] {msg['nickname']}: {msg['content']}")
        msg_text = "\n".join(msg_lines)

        prompt = f"""请分析以下聊天记录，提取关键信息。

【要求】
1. 提取 2-3 个话题（标题+简短描述+AI吐槽）
2. 提取 2-3 条金句（发言内容+发送者）
3. 列出发言最多的 {self.active_users_max} 位用户及发言数
4. 一句话锐评

【输出格式】（严格JSON）
{{
  "topics": [{{"title": "...", "detail": "...", "roast": "..."}}],
  "quotes": [{{"text": "...", "sender": "..."}}],
  "active_users": [{{"name": "...", "count": 0}}],
  "sharp_comment": "..."
}}

【聊天记录】
{msg_text}
"""
        return await self._call_llm(prompt)

    # ============================================================
    # 增量分析核心
    # ============================================================

    async def _do_incremental_analysis(self, group_id: str) -> bool:
        try:
            last_time = await self.db.get_last_incremental_time(group_id)
            if last_time == 0:
                last_time = int(time.time()) - self.window_hours * 3600

            messages = await self.db.get_messages_since(group_id, last_time)
            if len(messages) < self.incremental_min_messages:
                self._log(f"增量分析跳过：群 {group_id} 新增消息不足 {self.incremental_min_messages} 条")
                return False

            if len(messages) > self.max_messages_per_analysis:
                messages = messages[-self.max_messages_per_analysis:]

            result = await self._call_llm_incremental(messages)
            if not result:
                return False

            await self.db.save_incremental_batch(
                session_id=group_id,
                start_timestamp=last_time,
                end_timestamp=messages[-1]['timestamp'],
                message_count=len(messages),
                participants=len(set(m['user_id'] for m in messages)),
                topics=result.get('topics', []),
                quotes=result.get('quotes', []),
                active_users=result.get('active_users', []),
                sharp_comment=result.get('sharp_comment', '')
            )

            await self.db.update_last_incremental_time(group_id, messages[-1]['timestamp'])
            self._log(f"增量分析成功：群 {group_id}，{len(messages)} 条消息")
            return True
        except Exception as e:
            logger.error(f"[KiraDaily] 增量分析失败 {group_id}: {e}")
            return False

    # ============================================================
    # 增量批次合并
    # ============================================================

    async def _merge_batches(self, group_id: str) -> Optional[dict]:
        cutoff = int(time.time()) - self.window_hours * 3600
        batches = await self.db.get_incremental_batches(group_id, cutoff)
        if not batches:
            return None

        total_messages = sum(b['message_count'] for b in batches)
        all_users = {}
        all_topics = []
        all_quotes = []
        all_sharp_comments = []

        for batch in batches:
            try:
                topics = json.loads(batch['topics_json'])
                quotes = json.loads(batch['quotes_json'])
                users = json.loads(batch['active_users_json'])
                for u in users:
                    all_users[u['name']] = all_users.get(u['name'], 0) + u['count']
                all_topics.extend(topics)
                all_quotes.extend(quotes)
                if batch.get('sharp_comment'):
                    all_sharp_comments.append(batch['sharp_comment'])
            except Exception as e:
                self._log(f"解析批次失败: {e}")

        seen_titles = set()
        unique_topics = []
        for topic in all_topics:
            if topic.get('title') and topic['title'] not in seen_titles:
                seen_titles.add(topic['title'])
                unique_topics.append(topic)

        seen_texts = set()
        unique_quotes = []
        for quote in all_quotes:
            if quote.get('text') and quote['text'] not in seen_texts:
                seen_texts.add(quote['text'])
                unique_quotes.append(quote)

        sorted_users = sorted(all_users.items(), key=lambda x: x[1], reverse=True)
        top_users = [{'name': name, 'count': count} for name, count in sorted_users[:self.active_users_max]]

        return {
            'stats': {
                'total_messages': total_messages,
                'participants': len(all_users),
                'peak_hour': '00:00-23:59'
            },
            'topics': unique_topics[:self.topic_max],
            'active_users': top_users,
            'quotes': unique_quotes[:self.quote_max],
            'best_quote': None,
            'sharp_comment': all_sharp_comments[-1] if all_sharp_comments else '',
            'header_comment': f"📊 基于 {len(batches)} 个时段汇总的群聊日报"
        }

    # ============================================================
    # 生成报告（图片/文本）
    # ============================================================

    async def _generate_report(self, group_id: str, analysis_result: dict, messages: List[dict],
                               truncated: bool, start_time: int, end_time: int) -> Path:
        group_name = await self._get_group_name(group_id)
        group_avatar = await self.avatar_cache.get_group_avatar(group_id)

        border_colors = ["#D4A574", "#C9B0A0", "#E8C8A0", "#D4B8A8", "#C4A88C", "#E0C8B8", "#F5D0B0", "#D9B8A0"]
        group_avatar_border_color = random.choice(border_colors)
        title_colors = ["#6B4A3A", "#A67B5B", "#8B6B4A", "#C49A6C", "#B8896A", "#9C7A5A", "#7A5A4A"]
        title_color = random.choice(title_colors)
        title_gradient = f"linear-gradient(135deg, {random.choice(title_colors)}, {random.choice(title_colors)})"
        title_fonts = [
            "'Comic Sans MS', cursive", "'KaiTi', '楷体', serif",
            "'STZhongsong', '华文中宋', serif", "'Yuanti SC', '圆体', sans-serif",
            "'ZCOOL KuaiLe', cursive", "'Ma Shan Zheng', cursive"
        ]
        title_font = random.choice(title_fonts)

        nickname_to_userid = {}
        for msg in reversed(messages):
            if msg['nickname'] not in nickname_to_userid:
                nickname_to_userid[msg['nickname']] = msg['user_id']

        user_avatars = {}
        for user in analysis_result.get('active_users', []):
            name = user.get('name', '')
            user_id = nickname_to_userid.get(name, '')
            if user_id and user_id.isdigit():
                avatar = await self.avatar_cache.get_user_avatar(user_id)
                user_avatars[name] = avatar
            else:
                user_avatars[name] = self._bot_avatar or await self.avatar_cache.get_bot_avatar('0')
        for quote in analysis_result.get('quotes', []):
            sender = quote.get('sender', '')
            if sender and sender not in user_avatars:
                user_id = nickname_to_userid.get(sender, '')
                if user_id and user_id.isdigit():
                    avatar = await self.avatar_cache.get_user_avatar(user_id)
                    user_avatars[sender] = avatar
                else:
                    user_avatars[sender] = self._bot_avatar or await self.avatar_cache.get_bot_avatar('0')
        if analysis_result.get('best_quote'):
            sender = analysis_result['best_quote'].get('sender', '')
            if sender and sender not in user_avatars:
                user_id = nickname_to_userid.get(sender, '')
                if user_id and user_id.isdigit():
                    avatar = await self.avatar_cache.get_user_avatar(user_id)
                    user_avatars[sender] = avatar
                else:
                    user_avatars[sender] = self._bot_avatar or await self.avatar_cache.get_bot_avatar('0')

        # ============================================================
        # 活跃用户样式：使用纯色（单色）
        # ============================================================
        border_colors_list = [
            "#D4A574", "#C9B0A0", "#E8C8A0", "#D4B8A8", "#C4A88C", "#E0C8B8",
            "#F5D0B0", "#D9B8A0", "#CFAFA0", "#E6C8B0", "#DDBFA8", "#F0D0C0"
        ]
        user_styles = {}
        shuffled_colors = WARM_COLORS.copy()
        random.shuffle(shuffled_colors)
        for idx, user in enumerate(analysis_result.get('active_users', [])):
            name = user.get('name', '')
            border_color = random.choice(border_colors_list)
            color = shuffled_colors[idx % len(shuffled_colors)]
            user_styles[name] = {
                "border_color": border_color,
                "name_color": color
            }

        if not self._bot_avatar:
            await self._fetch_bot_info()

        stats = analysis_result.get('stats', {})
        stats['max_messages'] = self.max_messages_per_analysis if truncated else None
        stats['start_time'] = datetime.fromtimestamp(start_time).strftime('%m-%d %H:%M')
        stats['end_time'] = datetime.fromtimestamp(end_time).strftime('%m-%d %H:%M')
        analysis_result['stats'] = stats

        html = self.renderer.render_template(
            group_id=group_id,
            group_name=group_name,
            group_avatar=group_avatar,
            group_avatar_border_color=group_avatar_border_color,
            title_color=title_color,
            title_gradient=title_gradient,
            title_font=title_font,
            analysis=analysis_result,
            user_avatars=user_avatars,
            user_styles=user_styles,
            bot_avatar=self._bot_avatar or '',
            bot_nickname=self._bot_nickname or 'AI',
            generate_time=datetime.now(),
            topic_min=self.topic_min,
            topic_max=self.topic_max,
            quote_max=self.quote_max,
            active_users_max=self.active_users_max
        )

        html_path = self.reports_dir / f"{group_id.replace(':', '_')}_{int(time.time())}.html"
        html_path.write_text(html, encoding='utf-8')

        if self.output_format == 'image':
            png_path = html_path.with_suffix('.png')
            await self.renderer.render_to_image(html, str(png_path))
            return png_path
        else:
            txt_path = html_path.with_suffix('.txt')
            txt_content = self._generate_text_report(analysis_result)
            txt_path.write_text(txt_content, encoding='utf-8')
            return txt_path

    def _generate_text_report(self, analysis_result: dict) -> str:
        stats = analysis_result.get('stats', {})
        topics = analysis_result.get('topics', [])
        active_users = analysis_result.get('active_users', [])
        quotes = analysis_result.get('quotes', [])
        sharp_comment = analysis_result.get('sharp_comment', '')
        header_comment = analysis_result.get('header_comment', '')

        lines = []
        lines.append(f"📊 {header_comment}")
        total = stats.get('total_messages', 0)
        max_msgs = stats.get('max_messages')
        if max_msgs:
            lines.append(f"总消息数: {total}/{max_msgs} (截断)")
        else:
            lines.append(f"总消息数: {total}")
        lines.append(f"参与人数: {stats.get('participants', 0)}")
        lines.append(f"最活跃时段: {stats.get('peak_hour', '未知')}")
        if stats.get('start_time') and stats.get('end_time'):
            lines.append(f"统计范围: {stats['start_time']} ~ {stats['end_time']}")
        lines.append("")
        lines.append("💬 热门话题:")
        for topic in topics[:self.topic_max]:
            lines.append(f"  • {topic.get('title', '')}: {topic.get('detail', '')}")
            if topic.get('roast'):
                lines.append(f"    🤖 吐槽: {topic.get('roast')}")
        lines.append("")
        lines.append("👤 活跃用户:")
        for user in active_users[:self.active_users_max]:
            lines.append(f"  • {user.get('name', '')}: {user.get('count', 0)}条")
        lines.append("")
        if quotes:
            lines.append("💡 金句:")
            for q in quotes[:self.quote_max]:
                lines.append(f"  • {q.get('sender', '')}: {q.get('text', '')}")
        best = analysis_result.get('best_quote')
        if best:
            lines.append(f"  ⭐ 最佳: {best.get('sender', '')}: {best.get('text', '')}")
            lines.append(f"     理由: {best.get('reason', '')}")
        lines.append("")
        if sharp_comment:
            lines.append(f"🤖 锐评: {sharp_comment}")
        return "\n".join(lines)

    async def _send_report(self, group_id: str, report_path: Path):
        adapter = self.ctx.adapter_mgr.get_adapter("qq")
        if not adapter:
            logger.error("[KiraDaily] 无法获取QQ适配器")
            return
        parts = group_id.split(":")
        if len(parts) >= 3:
            qq_group_id = parts[2]
        else:
            qq_group_id = group_id
        if self.output_format == "image":
            await adapter.send_group_message(qq_group_id, MessageChain([Image(str(report_path))]))
        else:
            text_content = report_path.read_text(encoding="utf-8")
            for chunk in [text_content[i:i+500] for i in range(0, len(text_content), 500)]:
                await adapter.send_group_message(qq_group_id, MessageChain([Text(chunk)]))
                await asyncio.sleep(0.5)

    # ============================================================
    # 分析入口（统一分发）
    # ============================================================

    async def _do_analysis(self, group_id: str, user_id: str = "system") -> bool:
        if self._is_in_cooldown(group_id, user_id):
            remaining = self.cooldown_hours - (time.time() - self._cooldown_map.get(group_id, 0)) / 3600
            if remaining < 0:
                remaining = 0
            await self._send_text_to_group(group_id, self.msg_cooldown.format(remaining=remaining))
            return True

        async with self._semaphore:
            try:
                if self._is_incremental_group(group_id):
                    self._log(f"增量模式：合并群 {group_id} 的批次")
                    result = await self._merge_batches(group_id)
                    if not result:
                        await self._send_text_to_group(group_id, "⚠️ 暂无增量数据，请等待定时分析完成")
                        return True
                    result['header_comment'] = f"📊 基于 {self.window_hours} 小时窗口的群聊日报"
                    report_path = await self._generate_report(group_id, result, [], False, 0, 0)
                    await self._send_report(group_id, report_path)
                    self._update_cooldown(group_id)
                    return True

                messages = await self.db.get_messages(group_id, int(time.time()) - self.analysis_days * 86400)
                total_msgs = len(messages)
                if total_msgs < self.min_messages_threshold:
                    await self._send_text_to_group(
                        group_id,
                        self.msg_too_few.format(count=total_msgs, threshold=self.min_messages_threshold)
                    )
                    return True

                truncated = False
                if total_msgs > self.max_messages_per_analysis:
                    messages = messages[-self.max_messages_per_analysis:]
                    truncated = True

                start_time = messages[0]['timestamp'] if messages else 0
                end_time = messages[-1]['timestamp'] if messages else 0

                prompt = await self._build_analysis_prompt(messages, truncated)
                result = await self._call_llm(prompt)
                if not result:
                    await self._send_text_to_group(group_id, "❌ LLM 分析失败，请重试")
                    return True

                stats = result.get('stats', {})
                stats['total_messages'] = total_msgs
                stats['participants'] = len(set(m['user_id'] for m in messages))
                result['stats'] = stats

                result.setdefault('header_comment', '今日群聊，热闹非凡！')
                result.setdefault('sharp_comment', '')
                if not result.get('topics'):
                    result['topics'] = []
                while len(result['topics']) < self.topic_min:
                    result['topics'].append({'title': '？', 'detail': '暂无足够话题', 'roast': ''})
                if self.quote_max > 0 and not result.get('quotes'):
                    result['quotes'] = []
                result['best_quote'] = result.get('best_quote')

                report_path = await self._generate_report(group_id, result, messages, truncated, start_time, end_time)
                await self._send_report(group_id, report_path)
                self._update_cooldown(group_id)
                return True

            except Exception as e:
                logger.error(f"[KiraDaily] 分析群 {group_id} 失败: {e}")
                await self._send_text_to_group(group_id, self.msg_failed.format(error=str(e)[:100]))
                return True

    # ============================================================
    # 定时任务：完整日报
    # ============================================================

    async def _scheduler_loop(self):
        while self._running:
            try:
                now = datetime.now()
                target_time = datetime.strptime(self.auto_analysis_time, "%H:%M")
                target_datetime = now.replace(hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0)
                if target_datetime <= now:
                    target_datetime += timedelta(days=1)
                wait_seconds = (target_datetime - now).total_seconds()
                self._log(f"距离下次自动日报还有 {wait_seconds:.0f} 秒")
                await asyncio.sleep(wait_seconds)
                if not self._running:
                    break
                await self._run_auto_analysis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f("[KiraDaily] 定时任务异常: {e}")
                await asyncio.sleep(60)

    async def _run_auto_analysis(self):
        self._log("开始执行自动日报分析")
        if self.auto_enabled_groups:
            groups = self.auto_enabled_groups
        else:
            if self.enabled_groups:
                groups = self.enabled_groups
            else:
                groups = await self.db.get_all_groups()
        if not groups:
            self._log("没有需要分析的群")
            return
        tasks = []
        for group_id in groups:
            tasks.append(self._do_analysis(group_id, "system"))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        self._log(f"自动日报完成: {success_count}/{len(groups)} 群成功")

    # ============================================================
    # 增量分析定时任务
    # ============================================================

    async def _incremental_scheduler_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self.incremental_interval_minutes * 60)
                if not self._running:
                    break
                await self._run_incremental_analysis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[KiraDaily] 增量调度异常: {e}")
                await asyncio.sleep(60)

    async def _run_incremental_analysis(self):
        self._log("开始执行增量分析")
        if self.incremental_group_list:
            groups = self.incremental_group_list
        else:
            if self.enabled_groups:
                groups = self.enabled_groups
            else:
                groups = await self.db.get_all_groups()
        if not groups:
            return
        tasks = []
        for group_id in groups:
            tasks.append(self._do_incremental_analysis(group_id))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        self._log(f"增量分析完成: {success_count}/{len(groups)} 群成功")

    # ============================================================
    # 命令处理
    # ============================================================

    @on.im_message(priority=Priority.MEDIUM)
    async def handle_command(self, event: KiraMessageEvent):
        if not self.enable_command_trigger or not event.is_group_message():
            return
        text = "".join(elem.text for elem in event.message.chain if isinstance(elem, Text)).strip()
        if text not in self.command_prefixes:
            return
        group_id = self._get_group_id_from_event(event)
        if not group_id:
            await self.ctx.message_processor.send_message_chain(
                session=self._get_sid(event),
                chain=MessageChain([Text(self.msg_not_group)])
            )
            event.discard(force=True)
            event.stop()
            return
        if not self._is_group_enabled(group_id):
            await self.ctx.message_processor.send_message_chain(
                session=self._get_sid(event),
                chain=MessageChain([Text(self.msg_not_enabled)])
            )
            event.discard(force=True)
            event.stop()
            return
        user_id = self._get_user_id_from_event(event)
        if self._is_in_cooldown(group_id, user_id):
            remaining = self.cooldown_hours - (time.time() - self._cooldown_map.get(group_id, 0)) / 3600
            if remaining < 0:
                remaining = 0
            await self._send_text_to_group(group_id, self.msg_cooldown.format(remaining=remaining))
            event.discard(force=True)
            event.stop()
            return
        await self.ctx.message_processor.send_message_chain(
            session=self._get_sid(event),
            chain=MessageChain([Text(self.msg_processing)])
        )
        await self._do_analysis(group_id, user_id)
        event.discard(force=True)
        event.stop()

    # ============================================================
    # LLM 工具
    # ============================================================

    @register_tool(
        name="generate_daily_report",
        description="为当前群生成一份群聊日报，包含消息统计、热门话题、活跃用户、金句和AI锐评。"
                   "当用户要求总结群聊、生成日报、看看今天的群聊情况时调用此工具。"
                   "注意：每次调用会消耗较多Token，请确认用户确实需要日报再调用。",
        params={
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "default": 1,
                    "description": "分析最近几天的消息，默认1天"
                }
            },
            "required": []
        }
    )
    async def tool_generate_report(self, event: KiraMessageBatchEvent, days: int = 1) -> str:
        if not self.enable_natural_language_trigger:
            await self._send_text_to_group(
                self._get_group_id_from_event(event) or "unknown",
                self.msg_natural_disabled
            )
            return "✅ 已向用户说明自然语言触发已禁用。"
        group_id = self._get_group_id_from_event(event)
        if not group_id:
            return "✅ 已向用户说明：请在群聊中使用此功能。"
        if not self._is_group_enabled(group_id):
            await self._send_text_to_group(group_id, self.msg_not_enabled)
            return "✅ 已向用户说明该群未启用日报功能。"
        original_days = self.analysis_days
        self.analysis_days = min(max(days, 1), 7)
        user_id = self._get_user_id_from_event(event)
        if self._is_in_cooldown(group_id, user_id):
            remaining = self.cooldown_hours - (time.time() - self._cooldown_map.get(group_id, 0)) / 3600
            if remaining < 0:
                remaining = 0
            self.analysis_days = original_days
            await self._send_text_to_group(group_id, self.msg_cooldown.format(remaining=remaining))
            return "✅ 冷却中，已向用户说明。"
        await self._do_analysis(group_id, user_id)
        self.analysis_days = original_days
        return "✅ 日报任务已完成处理，结果已发送到群聊。"

    # ============================================================
    # 辅助方法：保存报告元数据
    # ============================================================

    async def _save_report_metadata(self, group_id: str, report_path: Path, result: dict):
        meta_path = self.reports_dir / f"{report_path.stem}.meta.json"
        meta = {
            "group_id": group_id,
            "timestamp": int(time.time()),
            "file": str(report_path.name),
            "stats": result.get("stats", {}),
            "topics": result.get("topics", [])
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # ============================================================
    # 清理旧报告
    # ============================================================

    async def _cleanup_old_reports(self):
        try:
            files = list(self.reports_dir.glob("*.png")) + list(self.reports_dir.glob("*.html")) + list(self.reports_dir.glob("*.txt"))
            if not files:
                return
            files.sort(key=lambda f: f.stat().st_mtime)
            if len(files) > self.report_cleanup_count:
                to_delete = files[:self.report_cleanup_count]
                for f in to_delete:
                    try:
                        f.unlink(missing_ok=True)
                        meta_path = f.with_suffix(".meta.json")
                        meta_path.unlink(missing_ok=True)
                    except Exception as e:
                        self._log(f"删除旧报告失败 {f}: {e}")
                self._log(f"清理了 {len(to_delete)} 个旧报告")
        except Exception as e:
            logger.error(f"[KiraDaily] 清理报告失败: {e}")